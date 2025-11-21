# MATE Panel TOTP Typing Launcher

## Purpose
A simple MATE panel launcher that:
- Generates a TOTP code using `oathtool`
- Types it into the active window via `xdotool`

Useful for quick SSH/terminal logins or any workflow needing a one-click OTP.

---

## Usage
1. Focus the window where the OTP should go  
2. Click the launcher  
3. OTP is typed automatically and submitted  

---

## Creating the Launcher
1. Right-click the MATE panel → **Add to Panel…**  
2. Select **Custom Application Launcher**  
3. Fill in:

**Type**
```
Application
```

**Name**
```
OTP Login
```

**Command**  
(Insert your Base32 secret key)
```
bash -c 'xdotool type --window "$(xdotool getactivewindow)" "$(oathtool --totp -b YOUR_OATH_KEY_HERE)"; xdotool key Return'
```

**Icon**  
Pick any icon.

Click **OK** to save.

---

## Requirements
- `oathtool`
- `xdotool`
- MATE Desktop

---

## Security
Your OTP secret key is stored in the launcher command.  
Use only on trusted machines.
