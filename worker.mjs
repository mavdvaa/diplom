import { createClient } from 'redis'
import { createVerify, randomUUID, createHash } from 'crypto'
import { publicKey, sign } from './cryptoPackage.mjs'
import { scalingSHA, step, stepT } from './scaling.mjs'
import { pool, userVerification } from './db.mjs'


const client = createClient()
if (!client.isOpen) {
  await client.connect().catch(console.error);
}
//

// для пакетов
function verify(data, signature, publicKey) {
  const verifier = createVerify('SHA256')
  verifier.update(JSON.stringify(data))
  verifier.end()

  return verifier.verify(publicKey, signature, 'hex')
}

//perodic
export async function processPeriodicTask(context) {
  const workerId = `Agent-${Math.random().toString(36).substring(7)}`

  const rightNumbers = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 153, 370, 371, 407]

  const userId = Math.floor(Math.random() * (5 - 1 + 1)) + 1
  const check = Math.floor(Math.random() * (410 - 0 + 1)) + 0
  const timeStartPeriod = Date.now()

  const data = {
    userId,
    check,
    timeStartPeriod
  }

  const checkUserPeriod = await userVerification(userId)

  if (!checkUserPeriod) {
    console.log(`Periodic task: пользователь с id ${userId} не может выполнить задание, он не зарегистрирован.`)
    return null
  }


  const jobId = randomUUID()

  const periodTable = await pool.query(`
    INSERT INTO periodic_tasks (user_id, number, job_id)
    VALUES ($1, $2, $3)
    RETURNING *`,
    [userId, check, jobId])

  const signature = sign(data)

  const signedData = {
    ...data,
    signature,
    publicKey: publicKey.export({ type: 'spki', format: 'pem' }).toString(),
    jobId
  }

  await client.lPush('periodic-task', JSON.stringify(signedData))

  const task = await client.brPop('periodic-task', 10)
  if (!task) {
    return null
  }

  const nData = JSON.parse(task.element)

  const { signature: nSignature, publicKey: nPublicKey, jobId: nJobId, ...dataToVerify } = nData

  const isVerified = verify(dataToVerify, nSignature, nPublicKey)


  if (!isVerified) {
    console.log("Подпись для задачи periodic не прошла проверку")
  } else {
    console.log('Подпись для задачи periodic не прошла проверку')

    let number = check
    let number1 = number
    let j = 0
    const length = number.toString().length
    for (let i = 0; i < length; i++) {

      j += (number1 % 10) ** length
      number1 = Math.trunc(number1 / 10)
    }

    if (j == number) {
      console.log(`Число ${number} является числом Армстронга`)
      const periodTableResT = await pool.query(`
        UPDATE periodic_tasks
        SET result = true,
        status = true
        WHERE job_id = $1`,
        [jobId])

    } else {
      console.log(`число ${number} не является числом Армстронга`)

      const statusPeriodic = await pool.query(`
      UPDATE periodic_tasks
      SET status = true
      WHERE job_id = $1`,
        [jobId])
    }

    const taskTimePeriod = (Date.now() - timeStartPeriod) / 1000
    const addTimePeriod = await pool.query(`
      UPDATE periodic_tasks
      SET task_time = $1
      WHERE job_id = $2`,
      [taskTimePeriod, jobId])

    if (rightNumbers.includes(check)) {
      const checkResPeriodicT = await pool.query(`
      UPDATE periodic_tasks
      SET is_right = true
      WHERE job_id = $1 AND result = true`,
      [jobId])
    } else {
      const checkResPeriodicF = await pool.query(`
      UPDATE periodic_tasks
      SET is_right = true
      WHERE job_id = $1 AND result = false`,
      [jobId])
    }

    const periodTableRes = await pool.query(`
    UPDATE users
    SET 
    count_right_periodic_tasks = (
    SELECT COUNT(is_right)
    FROM periodic_tasks p
    WHERE p.user_id = users.id AND status = true AND is_right = true),

    count_periodic_tasks = (
    SELECT COUNT(status)
    FROM periodic_tasks p
    WHERE p.user_id = users.id AND status = true)`,
    )

    console.log('Задание periodic выполнено')

    return nData
  }
}

// triggered
export async function processTriggeredTask(context, start) {

  const workerId = `Agent-${Math.random().toString(36).substring(7)}`

  let results = []

    const task = await client.brPopLPush('triggered-task', 'triggered-processing', 1)

    if (!task) {
      return null
    }

    const data = JSON.parse(task)

    const { signature, publicKey, userId, difficulty, timeStart, jobId } = data

    const dataToVerify = { userId, difficulty, timeStart }

    const isVerified = verify(dataToVerify, signature, publicKey)

    if (!isVerified) {
      console.log("Подпись для задачи triggered не прошла проверку")
    } else {
      console.log(`Подпись для задачи triggered прошла проверку`)

      const result = []

      function isSimple(i) {
        if (i < 2) return false;
        if (i == 2 || i == 3) return true;
        if (i % 2 == 0 || i % 3 == 0) return false;
        for (let j = 5; j ** 2 <= i; j += 6) {
          if (i % j == 0 || i % (j + 2) == 0) return false;
        }
        return true;
      }

      for (let i = start; i < (start + stepT[difficulty - 1]); i++) {
        if (isSimple(i)) {
          result.push(i);
        }
      }
      const taskTime = (Date.now() - timeStart) / 1000
      await client.set('taskTime', taskTime)

      console.log(`Результат от worker ${workerId} для задачи triggered: ${result.length}`)
      const lengthResult = `results`
      await client.lPush(lengthResult, result.map(String))
    }
    await client.lRem('triggered-processing', 0, task)
    results.push(data.task)

  return results

}

// воркер sha
export async function processShaTask(context, start) {

  const workerId = `Agent-${Math.random().toString(36).substring(7)}`;

  const task = await client.brPopLPush('sha-task', 'sha-processing', 1);

  if (!task) {
    return null;
  }

  const data = JSON.parse(task);
  const { signature, publicKey, text, userId, difficulty, jobId, timeStartSHA } = data

  const dataToVerify = { userId, text, difficulty, timeStartSHA }

  const isVerified = verify(dataToVerify, signature, publicKey)

  if (!isVerified) {
    console.log('Подпись для задачи sha не прошла проверку')
  } else {
    console.log(`Подпись для задачи sha прошла проверку`)
  }

  if (isVerified) {
    const end = start + step[(difficulty - 1)]

    const prefix = scalingSHA(difficulty)
    console.log(prefix)

    let result = []

    const fStart = start

    while (start < end) {
      const done = await client.get(`Завершена ${jobId}`)
      if (done) {
        break
      }
      const hash = createHash('sha256')
        .update(`${text}${start}`)
        .digest('hex')

      if (hash.startsWith(prefix)) {
        const taskTimeSHA = (Date.now() - timeStartSHA) / 1000
        result = { success: true, start, hash }
        console.log(`Worker ${workerId} завершил ${start - fStart} из ${step[difficulty-1]} попыток для задачи sha. Результат: ${hash}`)
        await client.setEx(`Завершена ${jobId}`, 60, "true")
        const shaTableRes = await pool.query(`
          UPDATE sha_tasks
          SET prefix = $1,
          result = $2,
          status = true,
          task_time = $3
          WHERE job_id = $4`,
          [prefix, hash, taskTimeSHA, jobId])

        const checkResSha = await pool.query(`
          UPDATE sha_tasks
          SET is_right = true
          WHERE job_id = $1 AND 
          LEFT(result, $2) = $3`,
          [jobId, prefix.length, prefix])

        const shaTableUsers = await pool.query(`
            UPDATE users
            SET 
            count_right_sha_tasks = (
            SELECT COUNT(is_right)
            FROM sha_tasks s
            WHERE s.user_id = users.id AND status = true AND is_right = true),
      
            count_sha_tasks = (
            SELECT COUNT(status)
            FROM sha_tasks s
            WHERE s.user_id = users.id AND status = true)`,
        )

        break
      }

      if (start % 1000 == 0) {
        console.log(`worker ${workerId} закончил попытку ${start} для задачи sha`)
      }
      start++
    }
  }

  await client.lRem('sha-processing', 0, task)
  return jobId
}

