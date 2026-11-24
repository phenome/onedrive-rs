# Register a Microsoft Entra desktop app

Register a free public-client app in a Microsoft Entra tenant where you can create app registrations. The registration supplies the application ID. Sign in to OneDrive with your personal Microsoft account.

1. Open the [Microsoft Entra admin center](https://entra.microsoft.com/) and go to **App registrations → New registration**. Give the app a name and select **Personal Microsoft accounts only**.
2. In **Authentication**, add the **Mobile and desktop applications** platform and register `http://localhost`. The app chooses an available localhost port for each sign-in. Leave **Certificates & secrets** empty. [Microsoft desktop app registration guide](https://learn.microsoft.com/en-us/entra/identity-platform/scenario-desktop-app-registration)
3. In **API permissions**, keep only the Microsoft Graph delegated permissions `Files.ReadWrite` and `offline_access`. Remove `User.Read` if the portal added it by default.
4. Copy the **Application (client) ID** from **Overview** into `$XDG_CONFIG_HOME/onedrive-rs/config.toml` (typically `~/.config/onedrive-rs/config.toml`).

   ```toml
   client_id = "00000000-0000-0000-0000-000000000000"
   ```

   Packagers can set `ONEDRIVE_CLIENT_ID` at build time to supply a default.

Sign in from your desktop session with `xdg-open`, a default web browser, and an unlocked Secret Service keyring such as GNOME Keyring available. The sign-in flow opens the system browser and receives the authorization code over localhost. The keyring holds the refresh token and replaces it when Microsoft rotates it. The access token lives in memory.
