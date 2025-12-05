package io.gtools

import gio.core.Result
import gio.core.Option
import groovy.util.logging.Slf4j
import io.gtools.types.Validator
import io.gtools.errors.InvalidCsrfTokenException

@Slf4j
class Action {

    enum ActionType {
        Validation, Satisfy, Run, RunWithForm
    }


    class FlowData<T> {
        T value
        protected final Closure<T> f
        protected String message
        protected Closure capture

        protected FlowData(Closure<T> f, Closure capture, String message) {
            this.f = f
            this.message = message
            this.capture = capture
        }

        def run() {
            def result = f.call()
            if (result instanceof Result<T>) {
                result.rethrow
                this.value = result.get()
            } else {
                this.value = result
            }
        }
    }

    static class ConditionUnsatisfied extends RuntimeException {
        ConditionUnsatisfied(String message, Throwable throwable) {
            super(message, throwable)
        }
    }

    private List<Validator> validators = []
    private Option<Closure> _allways = Option.ofNone()
    private Option<Closure> _successful = Option.ofNone()
    private List<FlowData> runs = []
    private Option<FlowData> _runWithForm = Option.ofNone()
    private Option<Closure> _catchAll = Option.ofNone()
    private Option<Closure> _errors = Option.ofNone()
    private List<FlowData> flowData = []

    private Object controller
    private Closure rule

    private List<String> propNames = []
    private Map<String, Object> propVals = [:]

    /**
     *
     * @param controller grails controller reference
     * @param f flow dsl
     */
    protected Action(Object controller, Closure f) {
        this.rule = f
        this.rule.delegate = this
        this.controller = controller
    }

    /**
     * Action flow. Order of execution:
     *
     * <ul>
     *     <li>{@code validate} seed validation dsl</li>
     *     <li>{@code errors} if validation fail and none validation declare a {@code errors} capture</li>
     *     <li>{@code satisfy}</li>
     *     <li>{@code run}</li>
     *     <li>{@code catchAll} if satisfy or run throw a exception</li>
     *     <li>{@code allways} </li>
     * </ul>
     * <pre>
     *     withFlow {
     *         validate {
     *             required var
     *             isTrue("my test") { 1 > 0 }
     *             errors { Collection col -> println "validation error $it" }
     *             valid { "do stuff in validation success " }
     *         }
     *         validate {other validation}
     *         errors { col -> println "all validation errors" }
     *         satisfy {condition}
     *         satisfy {condition} { ex -> error capture }
     *         satisfy 'my condition error description' {}
     *         run {...}
     *         run {...}
     *         runWithForm {}
     *         catchAll {}
     *         allways {}
     *     }
     * </pre>
     * @param f
     * @return
     */
    static withFlow(Object controller, @DelegatesTo(Action) Closure f) {
        new Action(controller, f).execute()
    }

    /**
     * Run flow
     * @return
     */
    private void execute() {

        // collect flow rule
        this.rule.call()

        // run all validations
        def invalids = this.validators
            .findAll {
                it.validate()
                it.isInvalid()
            }

        try {

            // run all conditions
            this.flowData.each {
                try {
                    it.run()
                } catch (Throwable e) {
                    log.error e.message, e
                    it.capture?.call(e)
                    throw new ConditionUnsatisfied(it.message ?: e.message, e)
                }
            }

            if (!invalids) {

                // run all "runs"
                this.runs.each {
                    it.run()
                }

                // run withForm (grais 3)
                this._runWithForm.foreach { fdata ->
                    this.controller
                        .withForm {
                            fdata.run()
                        }.
                        invalidToken {
                            throw new InvalidCsrfTokenException()
                        }
                }

                this._successful.foreach { it.call() }
            } else {

                // collect all validations messages
                def messages = invalids
                    .collect { it.messages }
                    .flatten()

                // if a validator has a invalid callback, so call it
                Option.of(invalids.find { it.hasErrorsCallback() })
                    .map { Validator it ->
                        Option.of(it.errorsCallback)
                            .orElse(this._errors) // or try local invalid callback
                    }
                    .foreach { Closure invalidCb ->
                        invalidCb.call(messages)
                    }
            }
        } catch (InvalidCsrfTokenException e) {
            this.controller?.flash?.invalidToken = true
            this._catchAll.foreach { it.call(e) }
        } catch (ConditionUnsatisfied e) {
            this._catchAll.foreach { it.call(e) }
        } catch (Throwable e) {
            log.error "$e.message", e
            this.controller?.flash?.exception = e
            this._catchAll.foreach { it.call(e) }
        } finally {
            this._allways.foreach { it.call() }
        }
    }

    /**
     * Execute a validation rule. If any validation declare {@code errors} capture, so validations erros
     * will not be captured by main flow errors capture. The {@code valid} code always is executed if validation pass.
     * @param f
     * @return
     */
    def validate(@DelegatesTo(Validator) Closure f) {
        this.validators << new Validator(f)
    }

    /**
     * Execute on try context. Can return a {@code Result}.
     * @param f
     * @return
     */
    FlowData run(Closure f) {
        this.runs << new FlowData(f, null, null)
        this.runs.last()
    }

    /**
     * Conditions before execute {@code run}. Can return a {@code Result}.
     * @param f
     * @return
     */
    FlowData satisfy(Closure f) {
        flowData << new FlowData(f, null, null)
        flowData.last()
    }

    /**
     * Conditions before execute {@code run}. Can return a {@code Result}.
     * @param f
     * @param capture Capture error, but NOT recover
     * @return
     */
    FlowData satisfy(Closure f, Closure capture) {
        flowData << new FlowData(f, capture, null)
        flowData.last()
    }

    /**
     * Conditions before execute {@code run}. Can return a {@code Result}.
     * @param message Message showed if error
     * @param f
     * @return
     */
    FlowData satisfy(String message, Closure f) {
        flowData << new FlowData(f, null, message)
        flowData.last()
    }

    /**
     * Fun {@code withForm} from grails controller
     * @param f
     * @return
     */
    FlowData runWithForm(Closure f) {
        this._runWithForm = Option.of(new FlowData(f, null, null))
        this._runWithForm.get()
    }

    /**
     * Run if success
     * @param f
     * @return
     */
    def successful(Closure f) {
        this._successful = Option.of(f)
    }

    /**
     * Catch errors, except validation errors. To catch validation erros, use {@code errors}
     * @param f
     * @return
     */
    def catchAll(Closure f) {
        this._catchAll = Option.of(f)
    }

    /**
     * Run allways, same if exception is raised
     * @param f
     * @return
     */
    def allways(Closure f) {
        this._allways = Option.of(f)
    }

    /**
     * Catch validation errors
     * @param f
     * @return
     */
    def errors(Closure f) {
        this._errors = Option.of(f)
    }

}
