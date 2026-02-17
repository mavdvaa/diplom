import crypto from 'crypto'
import { createClient } from 'redis'
import { initDB } from './db.mjs'
import { processPeriodicTask } from './worker.mjs'
import { publicKey, sign } from './cryptoPackage.mjs'
import { countT, SHAWorkers, triggWorkers, count, stepT, rightResTrigg } from './scaling.mjs'
import { pool, userVerification } from './db.mjs'

initDB()

//подключение к редис
const client = createClient()
if (!client.isOpen) {
  await client.connect().catch(err => console.log("Redis Connect Error:", err));
}

//функция
export const handler = async (event, context) => {
  if (event.httpMethod) {
    if (event.path == '/sha' && event.httpMethod == 'POST') {
      return await handleShaApi(event)
    }

    if (event.path == '/triggered' && event.httpMethod == 'POST') {
      return await triggeredTask(event)
    }

    if (event.path == '/users' && event.httpMethod == 'POST') {
      return await registration(event)
    }

    return { statusCode: 404 }
  }


  const functionName = context.functionName
  // periodic
  if (functionName.includes('periodicWorker')) {
    const periodicJob = await processPeriodicTask(context)
    return {
      body: JSON.stringify({
        periodic: periodicJob
      })
    }
  }
  // sha
  if (functionName.includes('workerAlpha')) {

    const checkQueueSha = await client.lLen('sha-task')

    if (checkQueueSha == 0) {
      console.log('Заданий нет')
    } else {

      const shadifficulty = await client.get('shaDifficulty')
      const difficulty = parseInt(shadifficulty)
      const result = await SHAWorkers(difficulty, context)

      return {
        body: JSON.stringify({
          sha: result
        })
      }
    }
  }
  // triggered
  if (functionName.includes('workerB')) {

    const lengthResult = `results`

    await client.del(lengthResult)

    const checkQueueTrigg = await client.lLen('triggered-task')


    if (checkQueueTrigg == 0) {
      console.log('Заданий нет')
    } else {

      const triggDifficulty = await client.get('triggDifficulty')
      const difficulty = parseInt(triggDifficulty)
      const taskTimeRes = await client.get('taskTime')

      const result = await triggWorkers(difficulty, context)

      const triggJobId = await client.get('triggJobId')

      const workersResults = await client.lRange(lengthResult, 0, -1)

      const sumNumbers = workersResults.reduce((accumulator, currentValue) => accumulator + Number(currentValue), 0)
      const bdResTrigg = await pool.query(`
        UPDATE triggered_tasks
        SET status = true,
        count = $1,
        sum = $2,
        task_time = $3
        WHERE job_id = $4`,
        [workersResults.length, sumNumbers, taskTimeRes, triggJobId])

      const checkResTrigg = await pool.query(`
        UPDATE triggered_tasks
        SET is_right = true
        WHERE job_id = $1 AND count = $2 AND sum  = $3`,
        [triggJobId, rightResTrigg[(difficulty - 1)][0], rightResTrigg[(difficulty - 1)][1]])

      const triggTableRes = await pool.query(`
        UPDATE users
        SET 
        count_right_triggered_tasks = (
        SELECT COUNT(is_right)
        FROM triggered_tasks t
        WHERE t.user_id = users.id AND status = true AND is_right = true),
      
        count_triggered_tasks = (
        SELECT COUNT(status)
        FROM triggered_tasks t
        WHERE t.user_id = users.id AND status = true)`,
      )

      console.log(`Всего простых чисел для задачи triggered: ${workersResults.length}`)
      console.log(`Сумма простых чисел для задачи triggered: ${sumNumbers}`)
      console.log(workersResults)

      return {
        body: JSON.stringify({
          triggered: result,
          nCount: workersResults.length
        })
      }
    }
  }
}



//регистрация
async function registration(event, context) {
  const { userId, password } = JSON.parse(event.body)

  const data = {
    userId,
    password
  }

  const userAuthorization = await userVerification(userId)

  if (!userAuthorization) {
    const user = await pool.query(`
      INSERT INTO users (id, password)
      VALUES ($1, $2)
      RETURNING *`,
      [userId, password])
    return {
      body: JSON.stringify({ message: `Пользователь ${userId} успешно добавлен` })
    }
  } else {
    return {
      body: JSON.stringify({ message: `Пользователь с id ${userId} уже существует` })
    }
  }
}

// post для triggeres
async function triggeredTask(event, context) {

  const checkQueueTriggTask = await client.lLen('triggered-task')
  const checkQueueTriggProcessing = await client.lLen('triggered-processing')

  if (checkQueueTriggTask > 0 || checkQueueTriggProcessing > 0) {
    return {
      body: JSON.stringify({ message: 'worker занят' })
    }
  }

  const { userId, difficulty } = JSON.parse(event.body)

  const jobId = crypto.randomUUID()

  const timeStart = Date.now()

  const data = {
    userId,
    difficulty,
    timeStart: timeStart
  }

  const checkUserTrigg = await userVerification(userId)
  const checkDifficiltyTrigg = (difficulty > 0 && difficulty < 4) ? true : false

  if (!checkDifficiltyTrigg) {
    return {
      body: JSON.stringify({ message: `Уровень сложности не должен быть меньше 1 или больше 3` })
    }
  }

  if (!checkUserTrigg) {
    return {
      body: JSON.stringify({ message: `Пользователя с id ${userId} не существует` })
    }
  }

  const triggeredTable = await pool.query(`
    INSERT INTO triggered_tasks (user_id, difficulty, range, job_id)
    VALUES ($1, $2, $3, $4)
    RETURNING *`,
    [userId, difficulty, stepT[difficulty - 1] + 1, jobId])

  const signature = sign(data)
  const signedData = {
    ...data,
    signature,
    publicKey: publicKey.export({ type: 'spki', format: 'pem' }).toString(),
    jobId
  }
  for (let i = 0; i < countT[(difficulty - 1)]; i++) {
    await client.lPush('triggered-task', JSON.stringify(signedData))
  }


  await client.set('triggJobId', jobId)
  await client.set('triggDifficulty', difficulty)
  await client.set('timeStart', timeStart)

  return {
    statusCode: 202,
    body: JSON.stringify({message: 'Задача успешно создана'})
  }
}


// post для sha
async function handleShaApi(event, context) {
  const checkQueueShaTask = await client.lLen('sha-task')
  const checkQueueShaProcessing = await client.lLen('sha-processing')

  if (checkQueueShaTask > 0 || checkQueueShaProcessing > 0) {
    return {
      body: JSON.stringify({ message: 'worker занят' })
    }
  }
  const { userId, text, difficulty } = JSON.parse(event.body)

  const checkUserSHA = await userVerification(userId)
  const checkDifficiltySHA = (difficulty > 0 && difficulty < 5) ? true : false

  if (!checkDifficiltySHA) {
    return {
      body: JSON.stringify({ message: `Уровень сложности не должен быть меньше 1 или больше 5` })
    }
  }

  if (!checkUserSHA) {
    return {
      body: JSON.stringify({ message: `Пользователя с id ${userId} не существует`})
    }
  }


  const jobId = crypto.randomUUID()

  const timeStartSHA = Date.now()

  const data = {
    userId,
    text,
    difficulty,
    timeStartSHA
  }

  const shaTable = await pool.query(`
    INSERT INTO sha_tasks (user_id, text, difficulty, job_id)
    VALUES ($1, $2, $3, $4)
    RETURNING *`,
    [userId, text, difficulty, jobId])

  const signature = sign(data)
  const signedData = {
    ...data,
    signature,
    publicKey: publicKey.export({ type: 'spki', format: 'pem' }).toString(),
    jobId
  }

  // кладем в очередь
  for (let i = 0; i < count[(difficulty - 1)]; i++) {
    await client.lPush('sha-task', JSON.stringify(signedData))
  }

  await client.set('shaDifficulty', difficulty)
  //для ограничения очереди
  await client.lTrim('sha-task', 0, 999)

  return {
    statusCode: 202,
    body: JSON.stringify({ message: 'Задача успешно создана' })
  }
}



