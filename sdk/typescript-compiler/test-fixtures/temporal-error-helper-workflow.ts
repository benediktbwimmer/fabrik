function errorMessage(error: unknown): string | undefined {
  if (typeof error === 'string') {
    return error
  }
  if (error instanceof Error) {
    return error.message
  }
  return undefined
}

export async function temporalErrorHelperWorkflow(): Promise<string | undefined> {
  return errorMessage('broken')
}
