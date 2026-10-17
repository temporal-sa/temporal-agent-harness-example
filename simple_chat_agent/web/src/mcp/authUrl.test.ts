import { describe, expect, it } from "vitest";
import { safeAuthUrl } from "./authUrl";

describe("safeAuthUrl", () => {
  it("allows https, and http only on this machine", () => {
    expect(safeAuthUrl("https://auth.example.com/authorize?x=1")).toBe("https://auth.example.com/authorize?x=1");
    expect(safeAuthUrl("http://127.0.0.1:9000/authorize")).toBe("http://127.0.0.1:9000/authorize");
    expect(safeAuthUrl("http://localhost/authorize")).toBe("http://localhost/authorize");
  });

  it("refuses other schemes and hosts", () => {
    for (const bad of ["javascript:alert(1)", "JavaScript:alert(1)", "data:text/html,hi", "http://auth.example.com/", "ftp://x/", "/relative", "", null, 42]) {
      expect(safeAuthUrl(bad)).toBeNull();
    }
  });
});
