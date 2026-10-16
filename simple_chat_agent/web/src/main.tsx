import { StrictMode } from "react";
import { createRoot } from "react-dom/client";

import "./theme/temporal-theme.css";
import "./theme/app.css";
import { App } from "./app/App";
import { CodingSessionProvider } from "./session";

const root = document.getElementById("root");
if (!root) throw new Error("index.html has no #root");

createRoot(root).render(
  <StrictMode>
    <CodingSessionProvider>
      <App />
    </CodingSessionProvider>
  </StrictMode>,
);
