export function LoginScreen({
  hidden,
  loggingIn,
  authMode,
  configured,
  subtitle,
  error,
  localUsername,
  localPassword,
  onLocalUsernameChange,
  onLocalPasswordChange,
  onLocalSubmit,
  onCatalogLoginClick,
}) {
  return (
    <section className="login-screen" hidden={hidden}>
      <div className={`login-card${loggingIn ? " logging-in" : ""}`}>
        <h1 aria-label="Advanced AI Agent"></h1>
        <p className="login-subtitle">{subtitle}</p>
        {authMode === "local" ? (
          <LocalLoginForm
            username={localUsername}
            password={localPassword}
            error={error}
            onUsernameChange={onLocalUsernameChange}
            onPasswordChange={onLocalPasswordChange}
            onSubmit={onLocalSubmit}
          />
        ) : (
          <CatalogLoginForm
            configured={configured}
            error={error}
            onLoginClick={onCatalogLoginClick}
          />
        )}
      </div>
    </section>
  );
}

function CatalogLoginForm({ configured, error, onLoginClick }) {
  return (
    <div className="login-form">
      <a
        className="login-button login-google"
        href="https://catalog.tmprl-demo.cloud"
        aria-disabled={configured ? undefined : "true"}
        onClick={onLoginClick}
      >
        Open Catalog
      </a>
      <LoginError error={error} />
    </div>
  );
}

function LocalLoginForm({
  username,
  password,
  error,
  onUsernameChange,
  onPasswordChange,
  onSubmit,
}) {
  return (
    <form className="login-form" onSubmit={onSubmit}>
      <label className="login-field">
        <span>Username</span>
        <input
          name="username"
          autoComplete="username"
          value={username}
          onChange={(event) => onUsernameChange(event.currentTarget.value)}
        />
      </label>
      <label className="login-field">
        <span>Password</span>
        <input
          name="password"
          type="password"
          autoComplete="current-password"
          value={password}
          onChange={(event) => onPasswordChange(event.currentTarget.value)}
        />
      </label>
      <button className="login-button" type="submit">
        Log In
      </button>
      <LoginError error={error} />
    </form>
  );
}

function LoginError({ error }) {
  return <p className="login-error">{error}</p>;
}
