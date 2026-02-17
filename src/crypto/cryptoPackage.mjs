import { generateKeyPairSync, createSign } from 'crypto'

// пакеты
export const { privateKey, publicKey } = generateKeyPairSync('rsa',
  {
    modulusLength: 2048,
  }
)

export function sign(data) {
  const sign = createSign('SHA256')
  sign.update(JSON.stringify(data))
  sign.end()

  return sign.sign(privateKey, 'hex')
}