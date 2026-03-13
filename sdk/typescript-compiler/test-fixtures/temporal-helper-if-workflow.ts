function prettyErrorMessage(message: string, err?: { message?: string; cause?: { message?: string } }) {
  let errMessage = err && err.message ? err.message : "";
  if (err && err.cause?.message) {
    errMessage = `${err.cause.message}`;
  }
  return `${message}: ${errMessage}`;
}

export async function temporalHelperIfWorkflow(message: string): Promise<string> {
  return prettyErrorMessage(message, { message: "fallback", cause: { message: "boom" } });
}
