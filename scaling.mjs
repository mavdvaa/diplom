import { processShaTask, processTriggeredTask } from "./worker.mjs"

//sha
export const step = [100, 500, 800, 850]
export const count = [1, 2, 25, 300]

// triggered
export const stepT = [99, 250, 500]
export const countT = [1, 4, 20]

export const rightResTrigg = [[25, 1060], [168, 76127], [1229, 5736396]]

// количество 0 в префиксе
export function scalingSHA(difficulty) {
    const prefix = '0'.repeat(parseInt(difficulty))
    return prefix
}

export async function SHAWorkers(difficulty, context) {
    let shaWorkers = []
    if (difficulty <= count.length) {
        shaWorkers = Array.from({ length: count[(difficulty - 1)] }, (_, i) =>
            processShaTask(context, (i * (step[difficulty - 1])))
        )
    }

    const promises = await Promise.all(shaWorkers)
    return promises
}

// диапазон для triggered

export function rangeTrigg(difficulty) {
    const start = 100
    const end = Number(start.toString() + '0'.repeat(difficulty))

    return end
}

export async function triggWorkers(difficulty, context) {
    let triggWorkersC = []
    triggWorkersC = Array.from({ length: countT[(difficulty - 1)] }, (_, i) =>
        processTriggeredTask(context, (i * (stepT[difficulty - 1]) + 1))
    )
    const promisesT = await Promise.all(triggWorkersC)
    return promisesT
}