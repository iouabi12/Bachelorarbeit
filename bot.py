import os
import pandas as pd
from alpaca_trade_api.rest import REST, TimeFrame
from openpyxl import load_workbook
from datetime import datetime, timedelta
import time
import pytz


# API-Schlüssel und geheimer Schlüssel für Alpaca
api_key = 'PKPAHSVJ03KSEAVKJ3DC'  # Dein API Key
api_secret = 'sexBtgi5wcqa5nL5bJSADenO30gZ7nJvLexdDzLO'  # Dein API Secret
base_url = 'https://paper-api.alpaca.markets'

# CET Zeitzone definieren
cet_tz = pytz.timezone('Europe/Berlin')

# Alpaca API-Client einrichten
api = REST(api_key, api_secret, base_url, api_version='v2')

# GUI-Label für Statusaktualisierungen
status_label = None  # Du musst dies in deinem GUI-Controller initialisieren

def update_gui_message(message):
    if status_label:
        status_label.config(text=message)
        status_label.update()
    print(message)  # Nachricht auch in der Konsole ausgeben

# Pfad zur Excel-Liste mit den Widerstandsniveaus
excel_file_path = r"C:\Users\49163\Desktop\alles\Krypto\All time High\Widerstandsniveaus.xlsx"

# Funktion zur Überprüfung aktiver Orders und Positionen
def has_active_orders_or_positions():
    # Überprüfung offener Orders
    open_orders = api.list_orders(status='open')
    if open_orders:
        update_gui_message(f"Es gibt offene Orders. Bot wird nicht ausgeführt.")
        return True
    
    # Überprüfung bestehender Positionen
    try:
        positions = api.list_positions()
        if positions:
            update_gui_message(f"Es gibt bestehende Positionen. Bot wird nicht ausgeführt.")
            return True
    except Exception as e:
        update_gui_message(f"Fehler beim Abrufen von Positionen: {e}")
        return False

    return False
# Funktion zum Überprüfen, ob die Kauforder ausgeführt wurde
def check_order_filled(order_id):
    try:
        order = api.get_order(order_id)
        if order.status == 'filled':
            return True
        else:
            return False
    except Exception as e:
        update_gui_message(f"Fehler beim Überprüfen des Orderstatus: {e}")
        return False
# Funktion zum Kaufen des gesamten verfügbaren Betrags, auch mit Bruchteilen
def buy_full_position(ticker, last_close):
    try:
        account = api.get_account()
        cash_available = float(account.cash)  # Verfügbarer Betrag in USD

        #print(f"Aktuelle Schluss-Kerze von 1-Stunden-Intervall: {last_close}")

        # Kaufpreis 0,10 % unter dem aktuellen Schlusskurs berechnen
        buy_price = round(last_close * (1 - 0.0005), 2)
        stop_loss_price = round(buy_price * (1 - 0.0088), 2)  # SL: -0,80% unter dem aktuellen Preis
        take_profit_price = round(buy_price * (1 + 0.0175), 2)  # TP: +1,45% über dem aktuellen Preis
        print(f"Kauf marketpreis bei {buy_price}")
        print('Take profit:',take_profit_price)
        print('stop loss:',stop_loss_price)
        
        # Berechnen der Anzahl an Aktien, die mit dem verfügbaren Kapital gekauft werden können
        quantity = int(cash_available // buy_price)  # Berechnung der Anzahl an Aktien (auch Bruchteile)
        if quantity > 0:
            # Erstellen einer Limit-Kauforder zum Kauf der Aktien 0,10 % unter dem aktuellen Preis
            order = api.submit_order(
                symbol=ticker,
                qty=quantity,  # Bruchteile von Aktien
                side='buy',
                type='market',  # Limit-Order für den Kauf
            )
            
            # Warten, bis die Kauforder ausgeführt wird
            update_gui_message(f"Warte auf Ausführung der Kauforder für {ticker}...")
            while not check_order_filled(order.id):
                time.sleep(60)  # Warten und den Orderstatus alle 5 Sekunden überprüfen

            
            # Erstellen der SL/TP Order über die neue Funktion
            submit_stop_loss_take_profit(ticker, quantity, stop_loss_price, take_profit_price)

            # Start der Überwachung und Erneuerung der Orders am nächsten Handelstag
            monitor_and_renew_orders(ticker, quantity, stop_loss_price, take_profit_price)

            update_gui_message(f"Limit-Kauforder für {quantity} Bruchteile von {ticker} zu {buy_price} USD pro Aktie gesetzt. "
                               f"SL bei {stop_loss_price} USD, TP bei {take_profit_price} USD gesetzt.")
        else:
            update_gui_message("Nicht genug Kapital, um Aktien zu kaufen.")

    except Exception as e:
        update_gui_message(f"Fehler beim Kauf der Position: {e}")



# Funktion zum Überprüfen, ob ein Ticker den Widerstand gebrochen hat
def handle_resistance_breakout(ticker, data, resistance_level,level_datetime, all_time_high):
    # Sicherstellen, dass die Daten korrekt auf die Handelszeiten von 15:30 bis 22:00 CET begrenzt sind
    market_open = datetime.now(cet_tz).replace(hour=15, minute=30, second=0, microsecond=0)
    market_close = datetime.now(cet_tz).replace(hour=22, minute=0, second=0, microsecond=0)

    # Filtere die Daten nur für die Marktzeiten
    data = data.between_time('15:30', '22:00')

    

    # Resample die Daten auf 1-Stunden-Kerzen ab 15:30 bis 22:00 CET mit dem richtigen Offset
    resampled_data = data.resample('60min', offset='30min').agg({
        'open': 'first',
        'high': 'max',
        'close': 'last'
    })

    # Sicherstellen, dass mindestens zwei Kerzen vorhanden sind
    if len(resampled_data) < 2:
        update_gui_message(f"Nicht genügend Daten für {ticker}, um die letzten beiden Kerzen zu analysieren.")
        return None
    highist_high = resampled_data['high'].iloc[:-1].max()  # Höchster High-Wert außer der letzten Kerze
    last_close = resampled_data['close'].iloc[-1]  # Letzte geschlossene Kerze
    last_open = resampled_data['open'].iloc[-1]  # Vorletzte Kerze
    last_close_time = resampled_data.index[-1]  # Zeitstempel der letzten Kerze
    last_open_time = resampled_data.index[-1]  # Zeitstempel der vorletzten Kerze

    # Differenz in Prozent berechnen zwischen Schlusskurs und Widerstandsniveau
    diff_percent = (last_close - resistance_level) / resistance_level * 100

    # Statusmeldung für das GUI aktualisieren
    update_gui_message(f"Prüfe {ticker} resistance_level bei :{resistance_level} am {level_datetime}")
    update_gui_message(f"----- {ticker} last close bei       :{last_close} am {last_close_time}")
    
    # Debugging-Ausgabe der Zeitstempel und Preise
    #print(f"all_time_high: {all_time_high}")
    #print(f"last_close_time: {last_close_time}, last_close: {last_close}")
    #print(f"last_open_time: {last_open_time}, last_open: {last_open}")

    # Überprüfen, ob der Widerstand über dem Allzeithoch liegt und die letzte Kerze den Widerstand durchbrochen hat
    if resistance_level > all_time_high: # last_close > resistance_level and last_open < resistance_level : # and resistance_level > all_time_high  and highist_high * 1.0005 < last_close
        return {
            'Ticker': ticker,
            'Resistance Level': resistance_level,
            'Diff Percent': diff_percent,
            'last Clos': last_close,
            'last_close_time': last_close_time
        }
    else:
        return None


# Funktion, um den Bot während der Marktzeiten laufen zu lassen (15:30-22:00 CET)
def run_bot_during_market_hours():
    update_gui_message("Lade Excel-Datei...")
    # Laden der Excel-Liste mit Widerstandsniveaus
    try:
        df = pd.read_excel(excel_file_path)
        update_gui_message(f"{len(df)} Tickers aus Excel geladen.")
    except Exception as e:
        update_gui_message(f"Fehler beim Laden der Excel-Datei: {e}")
        return

    # Aktuelle Uhrzeit und Handelszeiten definieren (CET)
    now = datetime.now(cet_tz)  # CET Zeit verwenden
    market_open = now.replace(hour=15, minute=30, second=0, microsecond=0)
    market_close = now.replace(hour=22, minute=0, second=0, microsecond=0)

    # Prüfen, ob wir uns innerhalb der Handelszeiten befinden
    if not (market_open <= now <= market_close):
        update_gui_message("Außerhalb der Marktzeiten. Bot wird nicht ausgeführt.")
        return
    
    # Liste für die Breakout-Ticker, die alle Bedingungen erfüllen
    breakout_tickers = []

    # Schleife über die Symbole in der Excel-Liste
    for ticker, group in df.groupby('Ticker'):
    # Für jedes Ticker-Symbol das größte Widerstandsniveau finden
        max_resistance_level = group['Resistance Level'].max()  # Größtes Widerstandsniveau
        all_time_high = group['All Time High'].max()  # Größtes All Time High für den Ticker
        max_resistance_row = group[group['Resistance Level'] == max_resistance_level]
        level_datetime = max_resistance_row['Date (CET)'].iloc[0]  # Datum der Zeile mit max_resistance_level

        
        now = datetime.now(cet_tz)
        today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        today_end = now  # Aktuelle Zeit
        # 1h-Daten abrufen ab dem All Time High Date bis heute
        try:
            data = api.get_bars(ticker, TimeFrame.Minute, start=today_start.isoformat(), end=today_end.isoformat(), limit=1000, feed='iex').df
            
            data.index = data.index.tz_convert('Europe/Berlin')
    
            # Filter für die Handelszeiten (15:30 bis 22:00 CET)
            trading_hours_bars = data.between_time('15:30', '22:00')
            resampled = trading_hours_bars.resample('60min', offset='30min').agg({
                'open' : 'first',
                'high': 'max',
                'close': 'last'
            })
            #print(f"now: {now}")
            last_candle_time = data.index[-1]
            plus =last_candle_time + timedelta(minutes=55)
            #print('last_candle_time', last_candle_time)
            #print('last_candle_time + eine stunde',plus)
            #if now < last_candle_time + timedelta(hours=1):
            #    resampled = resampled[:-1]  # Entferne die letzte, unvollständige Kerze
            print('++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++')
            #print(resampled)
            resampled.index = resampled.index.tz_convert('Europe/Berlin')  # Zeitstempel auf CET umstellen
        except Exception as e:
            update_gui_message(f"Fehler beim Abrufen der Daten für {ticker}: {e}")
            continue

        # Prüfen, ob der Ticker die Bedingungen erfüllt (nur das größte Widerstandsniveau)
        result = handle_resistance_breakout(ticker, resampled, max_resistance_level,level_datetime, all_time_high)
        if result:
            breakout_tickers.append(result)
            

    breakout_df = pd.DataFrame(breakout_tickers)
    
    # Setzen des Indexes auf den Ticker für eine klare Darstellung
    #breakout_df.set_index('Ticker', inplace=True)

    # Wenn es Breakout-Ticker gibt, diese analysieren
    print("\nBreakout-Ticker (Tabellenform):\n")
    print(breakout_df.to_string(index=True))  # Ausgabe als Tabelle mit Index
    if breakout_tickers:
        breakout_df = pd.DataFrame(breakout_tickers)

        # 0.15% bis 0.80%
        filtered_df = breakout_df[(breakout_df['Diff Percent'] >= 0.15) & (breakout_df['Diff Percent'] <= 0.80)].sort_values('Diff Percent')
        if not filtered_df.empty:
            ticker_to_buy = filtered_df.iloc[0]['Ticker']
            resistance_level = filtered_df.iloc[0]['Resistance Level']
            last_close = filtered_df.iloc[0]['last Clos']
            update_gui_message(f"Kaufe Ticker {ticker_to_buy}, Diff Percent: {filtered_df.iloc[0]['Diff Percent']:.2f}%, Schlusskurs: {last_close:.2f}")
            buy_full_position(ticker_to_buy, last_close)
        else:
            # >= 0.80 und <= 1.0
            filtered_df = breakout_df[(breakout_df['Diff Percent'] >= 0.80) & (breakout_df['Diff Percent'] <= 1.2)].sort_values('Diff Percent')
            if not filtered_df.empty:
                ticker_to_buy = filtered_df.iloc[0]['Ticker']
                resistance_level = filtered_df.iloc[0]['Resistance Level']
                last_close = filtered_df.iloc[0]['last Clos']
                update_gui_message(f"Kaufe Ticker {ticker_to_buy}, Diff Percent: {filtered_df.iloc[0]['Diff Percent']:.2f}%, Schlusskurs: {last_close:.2f}")
                buy_full_position(ticker_to_buy, last_close)
            else:
                # 0.05% bis 0.15%, absteigend sortieren
                filtered_df = breakout_df[(breakout_df['Diff Percent'] >= 0.05) & (breakout_df['Diff Percent'] < 0.15)].sort_values('Diff Percent', ascending=False)
                if not filtered_df.empty:
                    ticker_to_buy = filtered_df.iloc[0]['Ticker']
                    resistance_level = filtered_df.iloc[0]['Resistance Level']
                    last_close = filtered_df.iloc[0]['last Clos']
                    update_gui_message(f"Kaufe Ticker {ticker_to_buy}, Diff Percent: {filtered_df.iloc[0]['Diff Percent']:.2f}%, Schlusskurs: {last_close:.2f}")
                    buy_full_position(ticker_to_buy, last_close)
                else:
                    update_gui_message("Keine passenden Ticker gefunden.")
    else:
        update_gui_message("Keine Breakout-Ticker gefunden.")


# Funktion zur Überprüfung, ob es ein Handelstag ist (keine Samstage oder  Sonntage)
def is_trading_day():
    today = datetime.now(cet_tz)
    if today.weekday() >= 5:  # Samstag (5) oder Sonntag (6)
        return False
    # Optional: Feiertagsprüfung könnte hier hinzugefügt werden
    return True

# Bot starten, stündlich während der Handelszeiten, es sei denn, es gibt offene Orders oder Positionen
def start_bot():
    update_gui_message("Starte Bot...")

    # Prüfen, ob heute ein Handelstag ist
    if not is_trading_day():
        next_run_time = (datetime.now(cet_tz) + timedelta(days=(7 - datetime.now(cet_tz).weekday()))).replace(hour=16, minute=30, second=0)
        update_gui_message(f"Kein Handelstag heute. Nächste Prüfung am {next_run_time.strftime('%A, %d %B %Y um %H:%M:%S')}.")
        return

    if has_active_orders_or_positions():
        update_gui_message("Es gibt aktive Orders oder Positionen. Der Bot wird nicht weiter ausgeführt.")
        return

    while True:
        now = datetime.now(cet_tz)  # Aktuelle Zeit in CET
        market_open = now.replace(hour=15, minute=30, second=0, microsecond=0)
        market_close = now.replace(hour=22, minute=0, second=0, microsecond=0) 

        # Feste Ausführungszeiten: 
        fixed_hours = [16, 17, 18, 19, 20, 21]
        fixed_minutes = 30

        # Berechnen der nächsten Ausführungszeit
        next_run = None
        for hour in fixed_hours:
            next_run_candidate = now.replace(hour=hour, minute=fixed_minutes, second=0, microsecond=0)
            if now < next_run_candidate:
                next_run = next_run_candidate
                break

        # Wenn die aktuelle Zeit nach 19:30 ist, setze die nächste Prüfung auf den nächsten Tag um 16:30
        if not next_run:
            next_run = now.replace(hour=16, minute=30, second=0, microsecond=0) + timedelta(days=1)

        
        run_bot_during_market_hours()

        # Berechnen, wie lange bis zur nächsten Prüfung gewartet werden soll
        time_to_sleep = (next_run - now).total_seconds()

        # Prüfen, ob die nächste Prüfung nach Marktschluss wäre, falls ja, auf den nächsten Handelstag warten
        print(f"market_close:{market_close}")
        print(f"next_run: {next_run}")
        if next_run > market_close:
            # Auf den nächsten Handelstag warten
            next_run_time = market_open.strftime('%H:%M:%S')
            time_to_sleep = (market_open + timedelta(days=1) - now).total_seconds()
            update_gui_message(f"Markt geschlossen. Nächste Prüfung morgen um {next_run_time}. Zeit zum Schlafen: {time_to_sleep / 60 / 60:.2f} Stunden.")
            time.sleep(time_to_sleep)
            continue  # Wiederhole die Schleife am nächsten Tag
        # Ausgabe der aktuellen Zeit und der nächsten geplanten Ausführungszeit

        update_gui_message(f"Running at: {now.strftime('%H:%M:%S')}")
        update_gui_message(f"Nächste Prüfung um {next_run.strftime('%H:%M:%S')}. Zeit zum Schlafen: {(next_run - now).total_seconds() / 60:.2f} Minuten.")

        # Warten bis zur nächsten geplanten Ausführungszeit
        time.sleep(time_to_sleep)

def submit_stop_loss_take_profit(ticker, quantity, stop_loss_price, take_profit_price):
    try:
        # Überprüfe, ob du genug Aktien besitzt, um sie zu verkaufen
        position = api.get_position(ticker)
        available_quantity = float(position.qty)

        # Sicherstellen, dass die Verkaufsorders die gekaufte Menge nicht überschreiten
        if available_quantity < quantity:
            update_gui_message(f"Nicht genügend Aktien von {ticker}, um die Verkaufsorders zu platzieren. "
                               f"Verfügbare Menge: {available_quantity}")
            return

        # Erstellen einer OCO-Order für Take-Profit und Stop-Loss
        api.submit_order(
            symbol=ticker,
            qty=quantity,
            side='sell',  # Verkaufen der Position
            type='limit',  # Limit-Order
            time_in_force='gtc',  # Good-Till-Cancel (Order bleibt bis zur Ausführung gültig)
            order_class='oco',  # One Cancels Other (OCO)
            take_profit={
                "limit_price": take_profit_price  # Take-Profit-Preis
            },
            stop_loss={
                "stop_price": stop_loss_price,  # Stop-Loss-Preis
                "limit_price": stop_loss_price  # Optional: Stop-Limit-Preis (falls nötig)
            }
        )

        update_gui_message(f"SL/TP Order für {ticker} erstellt: SL bei {stop_loss_price}, TP bei {take_profit_price}.")

    except Exception as e:
        update_gui_message(f"Fehler beim Erstellen der SL/TP Order: {e}")

# Funktion zum erneuten Erstellen der Orders am nächsten Handelstag
def monitor_and_renew_orders(ticker, quantity, stop_loss_price, take_profit_price):
    # Überprüfe am Ende des Tages, ob die Orders ausgeführt wurden
    while True:
        now = datetime.now(cet_tz)  # Verwende CET-Zeitzone
        market_close_time = now.replace(hour=22, minute=0, second=0, microsecond=0)  # Marktschlusszeit 22:00 CET
        # Wenn der Markt geschlossen ist
        if now >= market_close_time:
            # Prüfe, ob die Orders ausgeführt wurden
            open_orders = api.list_orders(status='open')
            if any(order.symbol == ticker for order in open_orders):
                # Wenn es noch offene Orders gibt, lösche sie und erstelle sie am nächsten Handelstag neu
                for order in open_orders:
                    if order.symbol == ticker:
                        api.cancel_order(order.id)
                submit_stop_loss_take_profit(ticker, quantity, stop_loss_price, take_profit_price)
                time.sleep(86400)  # Schlafen für 24 Stunden, um am nächsten Handelstag neu zu starten
            else:
                update_gui_message(f"Alle Orders für {ticker} wurden ausgeführt.")
                break
        time.sleep(60)  # Überprüfung alle Minute


# Bot ausführen
start_bot()
