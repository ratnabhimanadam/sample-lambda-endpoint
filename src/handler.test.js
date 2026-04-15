const { handler } = require("./handler");

describe("Lambda handler", () => {
  it("returns greeting on GET /hello", async () => {
    const event = {
      requestContext: { http: { method: "GET", path: "/hello" } },
    };
    const res = await handler(event);
    expect(res.statusCode).toBe(200);
    const body = JSON.parse(res.body);
    expect(body.message).toBe("Hello from Lambda!");
  });

  it("returns personalised greeting on POST /hello", async () => {
    const event = {
      requestContext: { http: { method: "POST", path: "/hello" } },
      body: JSON.stringify({ name: "Alice" }),
    };
    const res = await handler(event);
    expect(res.statusCode).toBe(200);
    const body = JSON.parse(res.body);
    expect(body.message).toBe("Hello, Alice!");
  });

  it("returns 404 for unknown routes", async () => {
    const event = {
      requestContext: { http: { method: "GET", path: "/unknown" } },
    };
    const res = await handler(event);
    expect(res.statusCode).toBe(404);
  });
});
