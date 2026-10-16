import type { ReactNode } from "react";
import type { SessionStatus } from "../session";
import { StatusPill } from "./StatusPill";

export interface HeaderProps {
  title: string | null;
  demo: string | null;
  status: SessionStatus | null;
  views: ReactNode;
  presenter: boolean;
  onPresenter(): void;
  onMenu(): void;
}

export function Header({ title, demo, status, views, presenter, onPresenter, onMenu }: HeaderProps) {
  return (
    <header className="top">
      <button type="button" className="menu-btn" aria-label="Sessions and views" onClick={onMenu}>
        <i aria-hidden="true" />
      </button>
      <div className="brand">
        <span className="brand-mark" aria-hidden="true">
          T
        </span>
        <span className="brand-name">Coding agent</span>
      </div>
      {title && (
        <div className="session-title" title={title}>
          {title}
          {demo && <span className="demo"> · {demo}</span>}
        </div>
      )}
      {status && <StatusPill status={status} />}
      <div className="spacer" />
      {views}
      <button type="button" className="toggle" aria-pressed={presenter} onClick={onPresenter} title="Larger text for screen sharing">
        Presenter mode
      </button>
    </header>
  );
}
