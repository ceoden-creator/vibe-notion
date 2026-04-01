#!/usr/bin/env bun

import { internalRequest } from '@/platforms/notion/client'
import { handleDatabaseUpdateRow } from '@/platforms/notion/commands/database'
import { getCredentialsOrThrow, resolveAndSetActiveUserId } from '@/platforms/notion/commands/helpers'
import { formatPageUpdates } from '@/platforms/notion/formatters'
import { formatNotionId } from '@/shared/utils/id'
import { toOptionalString, toRecord, toStringArray, toStringValue } from '@/shared/utils/type-guards'

const WORKSPACE_ID = 'ff6a0cde-5e16-81d7-9ba9-0003b71a1bb2'
const DATABASE_ID = '2a7a0cde-5e16-8081-8b59-000bf1073808'
const VIEW_ID = '2bba0cde-5e16-8085-800c-000ccc74e30c'
const READY_DATE_PROPERTY_NAME = 'Ready Date'
const STATUS_PROPERTY_NAME = 'Статус'
const TARGET_STATUSES = new Set(['Готово', 'БАН', 'Проверка доков'])
const HISTORY_PAGE_SIZE = 50
const ROW_SYNC_CHUNK_SIZE = 100
const DEFAULT_TIME_ZONE = 'Asia/Saigon'

type ScriptOptions = {
  rowId?: string
  write: boolean
  overwriteExisting: boolean
  year: number
  timezone: string
}

type CollectionProperty = {
  name?: string
  type?: string
  [key: string]: unknown
}

type CollectionValue = {
  id: string
  schema?: Record<string, CollectionProperty>
}

type SyncCollectionResponse = {
  recordMap?: {
    collection?: Record<string, { value?: CollectionValue }>
  }
}

type SyncRecordValuesResponse = {
  recordMap?: {
    block?: Record<string, { value?: Record<string, unknown> }>
  }
}

type SyncViewResponse = {
  recordMap?: {
    collection_view?: Record<string, { value?: Record<string, unknown> }>
  }
}

type RowCandidate = {
  id: string
  title: string
  createdTime: number
  currentReadyDate?: unknown
  currentStatus?: string
}

type StatusMatch = {
  timestamp: number
  status: string
}

async function main(): Promise<void> {
  const options = parseArgs(process.argv.slice(2))
  const creds = await getCredentialsOrThrow()
  await resolveAndSetActiveUserId(creds.token_v2, WORKSPACE_ID)

  const collection = await fetchCollection(creds.token_v2, DATABASE_ID)
  const readyDatePropertyId = findPropertyIdByName(collection.schema ?? {}, READY_DATE_PROPERTY_NAME)
  if (!readyDatePropertyId) {
    throw new Error(`Property not found: ${READY_DATE_PROPERTY_NAME}`)
  }

  const rows = options.rowId
    ? [await fetchRow(creds.token_v2, options.rowId)]
    : await fetchRowsForYear(creds.token_v2, options.year, collection.schema ?? {})

  for (const row of rows) {
    await processRow(creds.token_v2, row, readyDatePropertyId, options)
  }
}

function parseArgs(args: string[]): ScriptOptions {
  let rowId: string | undefined
  let write = false
  let overwriteExisting = false
  let year = new Date().getFullYear()
  let timezone = DEFAULT_TIME_ZONE

  for (let i = 0; i < args.length; i++) {
    const arg = args[i]

    if (arg === '--row-id') {
      rowId = args[++i]
      continue
    }
    if (arg === '--write') {
      write = true
      continue
    }
    if (arg === '--overwrite-existing') {
      overwriteExisting = true
      continue
    }
    if (arg === '--year') {
      year = Number(args[++i])
      continue
    }
    if (arg === '--timezone') {
      timezone = args[++i]
      continue
    }
  }

  return {
    rowId: rowId ? formatNotionId(rowId) : undefined,
    write,
    overwriteExisting,
    year,
    timezone,
  }
}

async function fetchCollection(tokenV2: string, collectionId: string): Promise<CollectionValue> {
  const response = (await internalRequest(tokenV2, 'syncRecordValues', {
    requests: [{ pointer: { table: 'collection', id: collectionId }, version: -1 }],
  })) as SyncCollectionResponse

  const collection = Object.values(response.recordMap?.collection ?? {})[0]?.value
  if (!collection) {
    throw new Error(`Collection not found: ${collectionId}`)
  }

  return collection
}

function findPropertyIdByName(schema: Record<string, CollectionProperty>, propertyName: string): string | undefined {
  for (const [propertyId, property] of Object.entries(schema)) {
    if (property.name === propertyName) {
      return propertyId
    }
  }

  return undefined
}

async function fetchViewPageSort(tokenV2: string): Promise<string[]> {
  const response = (await internalRequest(tokenV2, 'syncRecordValues', {
    requests: [{ pointer: { table: 'collection_view', id: VIEW_ID }, version: -1 }],
  })) as SyncViewResponse

  const view = Object.values(response.recordMap?.collection_view ?? {})[0]?.value
  const pageSort = toStringArray(view?.page_sort)

  if (pageSort.length === 0) {
    throw new Error(`View has no rows: ${VIEW_ID}`)
  }

  return pageSort
}

async function fetchRowsForYear(
  tokenV2: string,
  year: number,
  schema: Record<string, CollectionProperty>,
): Promise<RowCandidate[]> {
  const rowIds = await fetchViewPageSort(tokenV2)
  const rows: RowCandidate[] = []

  for (let i = 0; i < rowIds.length; i += ROW_SYNC_CHUNK_SIZE) {
    const batchIds = rowIds.slice(i, i + ROW_SYNC_CHUNK_SIZE)
    const response = (await internalRequest(tokenV2, 'syncRecordValues', {
      requests: batchIds.map((id) => ({ pointer: { table: 'block', id }, version: -1 })),
    })) as SyncRecordValuesResponse

    for (const rowId of batchIds) {
      const record = response.recordMap?.block?.[rowId]?.value
      if (!record) continue

      const createdTime = typeof record.created_time === 'number' ? record.created_time : 0
      if (new Date(createdTime).getFullYear() !== year) {
        continue
      }

      rows.push(toRowCandidate(rowId, record, schema))
    }
  }

  return rows
}

async function fetchRow(tokenV2: string, rowId: string): Promise<RowCandidate> {
  const collection = await fetchCollection(tokenV2, DATABASE_ID)
  const response = (await internalRequest(tokenV2, 'syncRecordValues', {
    requests: [{ pointer: { table: 'block', id: rowId }, version: -1 }],
  })) as SyncRecordValuesResponse

  const record = response.recordMap?.block?.[rowId]?.value ?? Object.values(response.recordMap?.block ?? {})[0]?.value
  if (!record) {
    throw new Error(`Row not found: ${rowId}`)
  }

  return toRowCandidate(rowId, record, collection.schema ?? {})
}

function toRowCandidate(rowId: string, record: Record<string, unknown>, schema: Record<string, CollectionProperty>): RowCandidate {
  const titlePropId = findPropertyIdByName(schema, 'Аккаунт') ?? 'title'
  const readyDatePropId = findPropertyIdByName(schema, READY_DATE_PROPERTY_NAME)
  const statusPropId = findPropertyIdByName(schema, STATUS_PROPERTY_NAME)
  const properties = toRecord(record.properties) ?? {}

  return {
    id: rowId,
    title: extractPlainText(properties[titlePropId]),
    createdTime: typeof record.created_time === 'number' ? record.created_time : 0,
    currentReadyDate: readyDatePropId ? extractDateDescriptor(properties[readyDatePropId]) : undefined,
    currentStatus: statusPropId ? extractPlainText(properties[statusPropId]) : undefined,
  }
}

async function processRow(
  tokenV2: string,
  row: RowCandidate,
  readyDatePropertyId: string,
  options: ScriptOptions,
): Promise<void> {
  const match = await findFirstReadyTimestamp(tokenV2, row.id)

  const summary = {
    row_id: row.id,
    title: row.title,
    created_time: row.createdTime,
    current_status: row.currentStatus ?? null,
    current_ready_date: row.currentReadyDate ?? null,
    matched_status: match?.status ?? null,
    matched_timestamp: match?.timestamp ?? null,
    proposed_ready_date: match ? buildDateDescriptor(match.timestamp, options.timezone) : null,
    mode: options.write ? 'write' : 'dry-run',
  }

  if (!match) {
    console.log(JSON.stringify({ ...summary, skipped: 'no_matching_status_change' }))
    return
  }

  if (row.currentReadyDate && !options.overwriteExisting) {
    console.log(JSON.stringify({ ...summary, skipped: 'ready_date_already_set' }))
    return
  }

  if (!options.write) {
    console.log(JSON.stringify({ ...summary, action: 'would_update' }))
    return
  }

  await setReadyDate(tokenV2, row.id, readyDatePropertyId, match.timestamp, options.timezone)
  console.log(JSON.stringify({ ...summary, action: 'updated' }))
}

async function findFirstReadyTimestamp(tokenV2: string, rowId: string): Promise<StatusMatch | null> {
  let cursor = ''
  const matches: StatusMatch[] = []

  while (true) {
    const response = (await internalRequest(tokenV2, 'getActivityLog', {
      spaceId: WORKSPACE_ID,
      startingAfterId: cursor,
      navigableBlock: { id: rowId },
      limit: HISTORY_PAGE_SIZE,
    })) as Record<string, unknown>

    const updates = formatPageUpdates(response, HISTORY_PAGE_SIZE)

    for (const update of updates.results) {
      for (const edit of update.edits) {
        for (const propertyChange of edit.changed_properties ?? []) {
          if (propertyChange.property !== STATUS_PROPERTY_NAME) continue
          if (!TARGET_STATUSES.has(toStringValue(propertyChange.after.value))) continue

          matches.push({
            timestamp: edit.timestamp || update.start_time,
            status: toStringValue(propertyChange.after.value),
          })
        }
      }
    }

    if (!updates.has_more || !updates.next_cursor) {
      break
    }

    cursor = updates.next_cursor
  }

  if (matches.length === 0) {
    return null
  }

  matches.sort((a, b) => a.timestamp - b.timestamp)
  return matches[0]
}

async function setReadyDate(
  tokenV2: string,
  rowId: string,
  readyDatePropertyId: string,
  timestamp: number,
  timeZone: string,
): Promise<void> {
  const rowResponse = (await internalRequest(tokenV2, 'syncRecordValues', {
    requests: [{ pointer: { table: 'block', id: rowId }, version: -1 }],
  })) as SyncRecordValuesResponse

  const record = rowResponse.recordMap?.block?.[rowId]?.value ?? Object.values(rowResponse.recordMap?.block ?? {})[0]?.value
  const spaceId = toOptionalString(record?.space_id)
  if (!spaceId) {
    throw new Error(`Could not resolve space_id for row: ${rowId}`)
  }

  const dateDescriptor = buildDateDescriptor(timestamp, timeZone)

  await internalRequest(tokenV2, 'saveTransactions', {
    requestId: crypto.randomUUID(),
    transactions: [
      {
        id: crypto.randomUUID(),
        spaceId,
        operations: [
          {
            pointer: { table: 'block', id: rowId, spaceId },
            command: 'set',
            path: ['properties', readyDatePropertyId],
            args: [['‣', [['d', dateDescriptor]]]],
          },
        ],
      },
    ],
  })
}

function buildDateDescriptor(timestamp: number, timeZone: string): Record<string, string> {
  const date = new Date(timestamp)
  const parts = new Intl.DateTimeFormat('en-CA', {
    timeZone,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
  }).formatToParts(date)

  const lookup = Object.fromEntries(parts.map((part) => [part.type, part.value]))
  return {
    type: 'datetime',
    time_zone: timeZone,
    start_date: `${lookup.year}-${lookup.month}-${lookup.day}`,
    start_time: `${lookup.hour}:${lookup.minute}`,
  }
}

function extractPlainText(value: unknown): string {
  if (!Array.isArray(value)) return ''

  const parts: string[] = []
  for (const segment of value) {
    if (!Array.isArray(segment) || segment.length === 0) continue
    const text = segment[0]
    if (typeof text === 'string' && text !== '‣') {
      parts.push(text)
    }
  }

  return parts.join('')
}

function extractDateDescriptor(value: unknown): Record<string, unknown> | null {
  if (!Array.isArray(value)) return null

  for (const segment of value) {
    if (!Array.isArray(segment) || segment.length < 2) continue
    if (!Array.isArray(segment[1])) continue

    for (const decoration of segment[1]) {
      if (!Array.isArray(decoration) || decoration[0] !== 'd') continue
      const dateDescriptor = toRecord(decoration[1])
      if (dateDescriptor) {
        return dateDescriptor
      }
    }
  }

  return null
}

await main()
