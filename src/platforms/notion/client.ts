import { delay } from '@/shared/utils/delay'

let activeUserId: string | undefined

export function setActiveUserId(userId: string | undefined): void {
  activeUserId = userId
}

export function getActiveUserId(): string | undefined {
  return activeUserId
}

async function doRequest(tokenV2: string, endpoint: string, body: Record<string, unknown>): Promise<Response> {
  const headers: Record<string, string> = {
    'Content-Type': 'application/json',
    cookie: `token_v2=${tokenV2}`,
  }

  if (activeUserId) {
    headers['x-notion-active-user-header'] = activeUserId
    headers.cookie += `; notion_user_id=${activeUserId}`
  }

  return fetch(`https://www.notion.so/api/v3/${endpoint}`, {
    method: 'POST',
    headers,
    body: JSON.stringify(body),
  })
}

function buildErrorMessage(status: number, detail: string): string {
  const suffix = detail ? `: ${detail}` : ''
  return `Notion internal API error: ${status}${suffix}`
}

async function extractResponseDetail(response: Response): Promise<string> {
  try {
    const text = await response.text()
    if (text) {
      try {
        const json = JSON.parse(text)
        return json.message || json.msg || json.error || ''
      } catch {
        return text
      }
    }
  } catch {
    // could not read response body
  }
  return ''
}

const MAX_RATE_LIMIT_RETRIES = 3
const RATE_LIMIT_BASE_DELAY_MS = 1000

export async function internalRequest(
  tokenV2: string,
  endpoint: string,
  body: Record<string, unknown> = {},
): Promise<unknown> {
  for (let attempt = 0; attempt <= MAX_RATE_LIMIT_RETRIES; attempt++) {
    const response = await doRequest(tokenV2, endpoint, body)

    if (response.ok) {
      return response.json()
    }

    // On 429, retry with exponential backoff
    if (response.status === 429 && attempt < MAX_RATE_LIMIT_RETRIES) {
      const retryAfter = response.headers.get('retry-after')
      const retryAfterSeconds = retryAfter ? Number(retryAfter) : NaN
      const delayMs =
        Number.isFinite(retryAfterSeconds) && retryAfterSeconds > 0
          ? retryAfterSeconds * 1000
          : RATE_LIMIT_BASE_DELAY_MS * 2 ** attempt
      await delay(delayMs)
      continue
    }

    const detail = await extractResponseDetail(response)
    throw new Error(buildErrorMessage(response.status, detail))
  }

  throw new Error(`Notion internal API error: 429: Rate limited after ${MAX_RATE_LIMIT_RETRIES} retries`)
}
