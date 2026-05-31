export function LoginScreen({ hidden, loggingIn, configured, subtitle, error, onLoginClick }) {
  return (
    <section className="login-screen" hidden={hidden}>
      <div className={`login-card${loggingIn ? " logging-in" : ""}`}>
        <h1 aria-label="Simple Chat Agent"></h1>
        <p className="login-subtitle">{subtitle}</p>
        <div className="login-form">
          <a
            className="login-google"
            href="/oauth/google/start"
            aria-disabled={configured ? undefined : "true"}
            onClick={onLoginClick}
          >
            Log In
          </a>
          <p className="login-error">{error}</p>
        </div>
      </div>
    </section>
  );
}
