import { createContext, useContext } from "react";
import type { CodingSession } from "../session";

/** What rows deep in the transcript need from the shell. */
export interface TranscriptContextValue {
  session: CodingSession;
  /** Open a file in the workbench's Changes tab. */
  openFile(path: string): void;
  /** "GPT-5.5 (OpenAI)" for a provider and model id. */
  modelLabel(provider: string, model: string): string;
  /** "OpenAI" for a model name as the stream reports it, when known. */
  providerOfModel(model: string | null): string | null;
  postureLabel(id: string): string;
  workerAnswering: boolean;
}

export const TranscriptContext = createContext<TranscriptContextValue | null>(null);

export function useTranscript(): TranscriptContextValue {
  const v = useContext(TranscriptContext);
  if (!v) throw new Error("useTranscript outside TranscriptContext");
  return v;
}
