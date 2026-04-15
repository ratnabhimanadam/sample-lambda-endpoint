exports.handler = async (event) => {
  const method = event.requestContext?.http?.method ?? event.httpMethod;
  const path = event.requestContext?.http?.path ?? event.path;

  if (method === "GET" && path === "/hello") {
    return response(200, {
      message: "Hello from Lambda!",
      timestamp: new Date().toISOString(),
    });
  }

  if (method === "POST" && path === "/hello") {
    const body = JSON.parse(event.body || "{}");
    return response(200, {
      message: `Hello, ${body.name || "World"}!`,
      timestamp: new Date().toISOString(),
    });
  }

  return response(404, { error: "Not Found" });
};

function response(statusCode, body) {
  return {
    statusCode,
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  };
}
