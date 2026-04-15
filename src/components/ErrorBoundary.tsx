import { Component, type ReactNode } from "react";

interface ErrorBoundaryProps {
  children: ReactNode;
}

interface ErrorBoundaryState {
  hasError: boolean;
}

export class ErrorBoundary extends Component<ErrorBoundaryProps, ErrorBoundaryState> {
  state: ErrorBoundaryState = { hasError: false };

  static getDerivedStateFromError(): ErrorBoundaryState {
    return { hasError: true };
  }

  componentDidCatch(error: unknown) {
    // Keep a minimal log to aid runtime debugging in production.
    console.error("App runtime error caught by ErrorBoundary", error);
  }

  render() {
    if (this.state.hasError) {
      return (
        <div className="min-h-screen bg-[#070b14] px-6 py-10 text-slate-100">
          <div className="mx-auto max-w-2xl rounded-2xl border border-rose-500/30 bg-rose-500/10 p-6">
            <h1 className="text-xl font-semibold text-white">Something went wrong</h1>
            <p className="mt-2 text-sm text-rose-100/90">
              A runtime error occurred. Refresh the page to recover.
            </p>
          </div>
        </div>
      );
    }

    return this.props.children;
  }
}
