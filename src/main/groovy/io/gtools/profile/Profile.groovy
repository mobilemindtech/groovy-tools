package io.gtools.profile

import groovy.util.logging.Slf4j

import java.text.SimpleDateFormat;

@Slf4j
class Profile {

    def df = new SimpleDateFormat("HH:mm:ss.S")
    String name

    static Profile withProfile(String name) {
        new Profile(name)
    }

    /**
     * Create new profile
     * @param name Profine name
     */
    Profile(String name) {
        this.name = name
    }


    private List<Map> steps = new LinkedList()

    /**
     * Add step time
     * @param name Step name
     * @return
     */
    def step(String name = null) {

        def step = [name: name ?: "Point ${steps.size() + 1}", time: System.currentTimeMillis()]

        if (!steps.isEmpty()) {
            def last = steps.last
            showStepInfo(last, step)
        }

        steps << step
    }


    def leftShift(String name) {
        step(name)
    }

    /**
     * Show time from the beginning to the end
     * @param name End step name
     * @return
     */
    Profile finish() {

        if (steps.size() <= 1) {
            println("::> PROFILE[$name]: nothing to finish")
            return
        }

        def step = [name: "finish", time: System.currentTimeMillis()]

        def last = steps.last
        showStepInfo(last, step)

        def first = steps.first
        def time = step.time - first.time
        println("::> PROFILE[$name]: Finished! from ${first.name} to ${last.name} in ${time}/ms, ${time / 1000.0}/sec")
        this
    }

    /**
     * Resume all steps time
     * @return
     */
    def resume() {

        if (steps.size() <= 1) {
            println("::> PROFILE[$name]: nothing to resume")
            return
        }

        println("::> PROFILE[$name] RESUME BEGIN :::")
        Map last


        for (it in steps) {
            def startTime = new Date(it.time)
            if (!last) {
                println("::> Started ${it.name} at ${df.format(startTime)}")
            } else {
                def time = it.time - last.time
                println("::> ${last.name} to ${it.name} in ${time}/ms, ${time / 1000.0}/sec")
            }
            last = it
        }

        def first = steps.first
        def end = steps.last
        def time = end.time - first.time
        println("::> Finished all steps at ${df.format(new Date(end.time))} in ${time}/ms, ${time / 1000.0}/sec")
        println("::> PROFILE[$name] RESUME END :::")
    }

    Profile shortResume() {
        def first = steps.first
        def end = steps.last
        def time = end.time - first.time
        println("::> PROFILE[$name] RESUME BEGIN :::")
        println("::> Finished all steps at ${df.format(new Date(end.time))} in ${time}/ms, ${time / 1000.0}/sec")
        println("::> PROFILE[$name] RESUME END :::")
        this
    }

    void complete() {
        finish().shortResume()
    }


    private def showStepInfo(Map last, Map curr) {
        def time = curr.time - last.time
        println("::> PROFILE[${name}][${time / 1000}/sec]: ${last.name} to ${curr.name} in ${time}/ms")
    }
}
