import tkinter as tk
import subprocess
import threading
import os
import signal

# Skriptpfade
bot_script_path = r"C:\Users\49163\Desktop\alles\Krypto\py_skript\bot.py"
daily_ath_script_path = r"C:\Users\49163\Desktop\alles\Krypto\py_skript\all_time_high_d_inZeitraum.py"
hourly_resistance_script_path = r"C:\Users\49163\Desktop\alles\Krypto\py_skript\hourly_resistance.py"
close_positions_script_path = r"C:\Users\49163\Desktop\alles\Krypto\py_skript\close_all_position.py"
close_orders_script_path = r"C:\Users\49163\Desktop\alles\Krypto\py_skript\close_open_order.py"
automatic_ath_Scheduler_path = r"C:\Users\49163\Desktop\alles\Krypto\py_skript\automatic execute.py"

# Globale Prozesse
bot_process = None
daily_ath_process = None
hourly_resistance_process = None
automatic_ath_process = None

# Hilfsfunktion für die GUI-Ausgabe
def append_to_output(text):
    output_text.config(state="normal")
    output_text.insert(tk.END, text + "\n")
    output_text.see(tk.END)
    output_text.config(state="disabled")

# Skript ausführen
def run_process(script_path, label, start_callback, stop_callback):
    try:
        label.config(text=f"{os.path.basename(script_path)} läuft...", fg="green")
        process = subprocess.Popen(
            ["pythonw", script_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )

        def stream_output():
            for line in process.stdout:
                append_to_output(f"{os.path.basename(script_path)}: {line.strip()}")
            process.wait()
            label.config(text=f"{os.path.basename(script_path)} beendet", fg="red")
            stop_callback()

        threading.Thread(target=stream_output, daemon=True).start()
        start_callback(process)
    except Exception as e:
        append_to_output(f"Fehler beim Starten des Skripts {os.path.basename(script_path)}: {e}")

# Prozess stoppen
def stop_process(process, label, clear_process_callback):
    if process:
        try:
            os.kill(process.pid, signal.SIGTERM)
            append_to_output(f"Prozess {process.pid} beendet.")
            process.wait()
        except Exception as e:
            append_to_output(f"Fehler beim Beenden des Prozesses: {e}")
        finally:
            clear_process_callback()
            label.config(text="Pausiert", fg="red")
    else:
        append_to_output("Kein aktiver Prozess vorhanden.")

# Funktionen für spezifische Aktionen
def start_bot():
    global bot_process
    if bot_process is None:
        def start_callback(proc):
            global bot_process
            bot_process = proc

        def stop_callback():
            global bot_process
            bot_process = None

        run_process(
            bot_script_path, status_label_bot,
            start_callback,
            stop_callback
        )
    else:
        append_to_output("Bot läuft bereits")


def stop_bot():
    global bot_process
    if bot_process:
        try:
            # Beenden des Prozesses
            os.kill(bot_process.pid, signal.SIGTERM)
            append_to_output("Bot-Skript wurde gestoppt.")
            bot_process = None
            status_label_bot.config(text="gestoppt", fg="red")
        except Exception as e:
            append_to_output(f"Fehler beim Stoppen des Bots: {e}")
    else:
        append_to_output("Bot ist nicht aktiv")


def start_daily_ath():
    global daily_ath_process
    if daily_ath_process is None:
        def start_callback(proc):
            global daily_ath_process
            daily_ath_process = proc

        def stop_callback():
            global daily_ath_process
            daily_ath_process = None

        run_process(
            daily_ath_script_path, status_label_ath,
            start_callback,
            stop_callback
        )
    else:
        append_to_output("Daily ATH läuft bereits")


def stop_daily_ath():
    global daily_ath_process
    if daily_ath_process:
        try:
            # Beenden des Prozesses
            os.kill(daily_ath_process.pid, signal.SIGTERM)
            append_to_output("Daily ATH-Skript wurde gestoppt.")
            daily_ath_process = None
            status_label_ath.config(text="gestoppt", fg="red")
        except Exception as e:
            append_to_output(f"Fehler beim Stoppen des Daily ATH-Skripts: {e}")
    else:
        append_to_output("Daily ATH ist nicht aktiv")





def start_hourly_resistance():
    global hourly_resistance_process
    if hourly_resistance_process is None:
        def start_callback(proc):
            global hourly_resistance_process
            hourly_resistance_process = proc

        def stop_callback():
            global hourly_resistance_process
            hourly_resistance_process = None

        run_process(
            hourly_resistance_script_path, status_label_resistance,
            start_callback,
            stop_callback
        )
    else:
        append_to_output("Hourly Resistance läuft bereits")

def stop_hourly_resistance():
    global hourly_resistance_process
    if hourly_resistance_process:
        try:
            # Beenden des Prozesses
            os.kill(hourly_resistance_process.pid, signal.SIGTERM)
            append_to_output("Hourly Resistance-Skript wurde gestoppt.")
            hourly_resistance_process = None
            status_label_resistance.config(text="gestoppt", fg="red")
        except Exception as e:
            append_to_output(f"Fehler beim Stoppen des Skripts: {e}")
    else:
        append_to_output("Hourly Resistance ist nicht aktiv")


# Funktionen zum Schließen der Positionen und Orders
def run_close_positions():
    """Schließt alle offenen Positionen."""
    run_process(
        close_positions_script_path, status_label_positions,
        lambda proc: None,
        lambda: None
    )

def run_close_orders():
    """Storniert alle offenen Orders."""
    run_process(
        close_orders_script_path, status_label_positions,
        lambda proc: None,
        lambda: None
    )


def start_automatic_ath_Scheduler():
    global automatic_ath_process
    if automatic_ath_process is None:
        def start_callback(proc):
            global automatic_ath_process
            automatic_ath_process = proc

        def stop_callback():
            global automatic_ath_process
            automatic_ath_process = None

        run_process(
            automatic_ath_Scheduler_path, status_automatic_ath_Scheduler,
            start_callback,
            stop_callback
        )
    else:
        append_to_output("automatic_ath_Scheduler läuft bereits")

def stop_automatic_Scheduler():
    global automatic_ath_process
    if automatic_ath_process:
        try:
            # Beenden des Prozesses
            os.kill(automatic_ath_process.pid, signal.SIGTERM)
            append_to_output("automatic_ath_Scheduler wurde gestoppt.")
            automatic_ath_process = None
            status_automatic_ath_Scheduler.config(text="gestoppt", fg="red")
        except Exception as e:
            append_to_output(f"Fehler beim Stoppen des automatic_ath_Scheduler: {e}")
    else:
        append_to_output("automatic_ath_Scheduler ist nicht aktiv")


# GUI erstellen
root = tk.Tk()
root.title("Trading Bot Controller")

# Bot-Steuerung
tk.Label(root, text="Bot:", font=("Arial", 10, "bold")).grid(row=0, column=0, padx=5, pady=5)
tk.Button(root, text="Starten", command=start_bot).grid(row=0, column=1, padx=5, pady=5)
tk.Button(root, text="stoppen", command=stop_bot).grid(row=0, column=2, padx=5, pady=5)
status_label_bot = tk.Label(root, text="Bereit", fg="blue")
status_label_bot.grid(row=0, column=3, padx=5, pady=5)

# Daily All Time High-Steuerung
tk.Label(root, text="Daily ATH:", font=("Arial", 10, "bold")).grid(row=1, column=0, padx=5, pady=5)
tk.Button(root, text="Starten", command=start_daily_ath).grid(row=1, column=1, padx=5, pady=5)
tk.Button(root, text="stoppen", command=stop_daily_ath).grid(row=1, column=2, padx=5, pady=5)
status_label_ath = tk.Label(root, text="Bereit", fg="blue")
status_label_ath.grid(row=1, column=3, padx=5, pady=5)

# Hourly Resistance-Steuerung
tk.Label(root, text="Hourly Resistance:", font=("Arial", 10, "bold")).grid(row=2, column=0, padx=5, pady=5)
tk.Button(root, text="Starten", command=start_hourly_resistance).grid(row=2, column=1, padx=5, pady=5)
tk.Button(root, text="stoppen", command=stop_hourly_resistance).grid(row=2, column=2, padx=5, pady=5)
status_label_resistance = tk.Label(root, text="Bereit", fg="blue")
status_label_resistance.grid(row=2, column=3, padx=5, pady=5)

# Positionen schließen
tk.Label(root, text="Orders / Positionen:", font=("Arial", 10, "bold")).grid(row=3, column=0, padx=5, pady=5)
tk.Button(root, text="Orders stornieren", command=run_close_orders).grid(row=3, column=1, padx=5, pady=5)
tk.Button(root, text="Positionen schließen", command=run_close_positions).grid(row=3, column=2, padx=5, pady=5)
status_label_positions = tk.Label(root, text="Bereit", fg="blue")
status_label_positions.grid(row=3, column=3, padx=5, pady=5)

# Orders stornieren
#tk.Label(root, text="Orders:", font=("Arial", 10, "bold")).grid(row=4, column=0, padx=5, pady=5)

#status_label_orders = tk.Label(root, text="Bereit", fg="blue")
#status_label_orders.grid(row=4, column=3, padx=5, pady=5)

tk.Label(root, text="aut. ATH Script:", font=("Arial", 10, "bold")).grid(row=5, column=0, padx=5, pady=5)
tk.Button(root, text="Starten", command=start_automatic_ath_Scheduler).grid(row=5, column=1, padx=5, pady=5)
tk.Button(root, text="stoppen", command=stop_automatic_Scheduler).grid(row=5, column=2, padx=5, pady=5)
status_automatic_ath_Scheduler = tk.Label(root, text="Bereit", fg="blue")
status_automatic_ath_Scheduler.grid(row=5, column=3, padx=5, pady=5)



output_text = tk.Text(root, state="disabled", height=15, width=80, bg="#333333", fg="white", font=("Arial", 12))

output_text.grid(row=6, column=0, columnspan=5, padx=5, pady=10)

# Hauptschleife der GUI
root.mainloop()
