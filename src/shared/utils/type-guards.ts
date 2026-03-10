function toObjectRecord(value: unknown): Record<string, unknown> | undefined {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    return undefined
  }

  return value as Record<string, unknown>
}

export function toRecord(value: unknown): Record<string, unknown> | undefined {
  return toObjectRecord(value)
}

export function toRecordOrNull(value: unknown): Record<string, unknown> | null {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    return null
  }

  return value as Record<string, unknown>
}

export function toStringValue(value: unknown): string {
  return typeof value === 'string' ? value : ''
}

export const toStringOrEmpty = toStringValue

export function toOptionalString(value: unknown): string | undefined {
  return typeof value === 'string' ? value : undefined
}

export function toCursor(value: unknown): string | null {
  return toOptionalString(value) ?? null
}

export function toStringArray(value: unknown): string[] {
  if (!Array.isArray(value)) {
    return []
  }

  return value.filter((entry): entry is string => typeof entry === 'string')
}
