import backtrader as bt
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta, date
import pytz
import os
import time


take_profit=1.02
stop_loss=0.99
# CET-Zeitzone definieren
cet_tz = pytz.timezone('Europe/Berlin')

# Pfad zur Excel-Liste mit den Widerstandsniveaus
#excel_file_path = r"C:\Users\49163\Desktop\alles\Krypto\All time high\Bachelorarbeit\all_time_high\all_time_high.xlsx"
input_file_path = r"C:\Users\49163\Desktop\alles\Krypto\1.strategy_BB+ema200.xlsx"
# Pfad für das Excel-Output (Kapitalentwicklung und Ziele)
output_excel_file = r'C:\Users\49163\Desktop\alles\Krypto\All time High\Kapitalentwicklung_ema9_crossed_ema21_1%_2%.xlsx'
output_excel_file1 = r'C:\Users\49163\Desktop\alles\Krypto\All time High\filtered_df.xlsx'
output_excel_file2 = r'C:\Users\49163\Desktop\alles\Krypto\All time High\breakout_df.xlsx'


feiertage_2024 = [
    date(2024, 1, 1),   # Neujahrstag
    date(2024, 1, 15),  # Martin Luther King Jr. Day
    date(2024, 2, 19),  # Presidents' Day
    date(2024, 3, 29),  # Karfreitag
    date(2024, 5, 27),  # Memorial Day
    date(2024, 7, 4),   # Unabhängigkeitstag
    date(2024, 9, 2),   # Labor Day
    date(2024, 11, 28), # Thanksgiving
    date(2024, 12, 25)  # Weihnachten
]

# Funktion zum Laden der Excel-Daten
def load_resistance_data():
    #df = pd.read_excel(input_file_path)
    tickers_df = pd.read_excel(input_file_path, sheet_name='ema90', usecols=['Symbol'])
    return tickers_df




# Funktion, um die 5-Minuten-Daten von yfinance zu holen



####################################################################################
#####################################################################################
#######################################################################################



      

####################################################################################
#####################################################################################
#######################################################################################
####################################################################################
#####################################################################################
#######################################################################################

# Hauptfunktion, um die Strategie auszuführen
#from datetime import Timedelta
def is_handelstag(date):
    # Prüfe, ob der Tag ein Samstag (5) oder Sonntag (6) ist
    if date.weekday() >= 5:  # Samstag und Sonntag sind keine Handelstage
        return False
    # Prüfe, ob der Tag ein Feiertag ist
    if date.date() in feiertage_2024:
        return False
    return True



def add_handelstage(start_datetime, days_to_add):
    current_datetime = start_datetime
    days_added = 0
    
    while days_added < days_to_add:
        current_datetime += timedelta(days=1)  # Einen Tag hinzufügen
        if is_handelstag(current_datetime):    # Prüfen, ob es ein Handelstag ist
            days_added += 1
    print('add 2 handelstage', current_datetime)
    return current_datetime

def get_previous_handelstag(start_datetime):
    
    # Setze das aktuelle Datum auf den vorherigen Tag
    previous_datetime = start_datetime - pd.Timedelta(days=1)
    # Schleife, um den letzten Handelstag zu finden
    while not is_handelstag(previous_datetime):
        previous_datetime -= pd.Timedelta(days=1)  # Einen Tag rückwärts gehen
    
    return previous_datetime
#####################################################################################
#######################################################################################

############################################################################


#######################################################################
#######################################################################
tickers_no_data = []

# Funktion zum Speichern eines fehlenden Tickers in die Excel-Datei
def save_ticker_no_data(ticker, start_date, end_date, fehlertyp):
    if os.path.exists(output_excel_file1):
            df = pd.read_excel(output_excel_file1)
    else:
        df = pd.DataFrame(columns=['Ticker', 'start_date', 'end_date', 'fehlertyp'])
    new_data = {
        'Ticker': ticker,
        'start_date': start_date,
        'end_date': end_date,
        'fehlertyp' : fehlertyp
    }
    new_data_cleaned = {key: value for key, value in new_data.items() if pd.notna(value)}
        
    # Füge `new_data_cleaned` hinzu, wenn es gültige Daten enthält
    if new_data_cleaned:
        new_row = pd.DataFrame([new_data_cleaned])
        if not new_row.isna().all(axis=1).iloc[0]:
            df = pd.concat([df, new_row], ignore_index=True)
            print("Neue Zeile hinzugefügt:", new_row)
        else:
            print("Die neue Zeile enthält nur NaN-Werte und wurde nicht hinzugefügt.")
    else:
        print("Keine gültigen neuen Daten zum Hinzufügen.")
    
    df.to_excel(output_excel_file1, index=False)
    print(f"Ergebnisse in Excel gespeichert.")    

################

def run_backtrader_strategy(start_datetime, cash):
    print('-----------------------------------------------------------------------')
    print(start_datetime)
    print(cash)
    # Wenn das Startdatum das Enddatum überschreitet, beenden
    if pd.Timestamp(start_datetime) >= pd.Timestamp(global_end_datetime):  
        print(f"Enddatum {global_end_datetime} erreicht. Keine weiteren Versuche möglich.")
        return
    
    if start_datetime.hour >= 21 and start_datetime.hour < 24:
        # Wenn es nach 22:00 oder vor 15:00 ist, setze den Start auf 15:30 des nächsten Tages
        start_datetime = start_datetime.replace(hour=16, minute=30) + timedelta(days=1)
        print('start_datetime is ', start_datetime)
    elif start_datetime.hour >= 14 and start_datetime.hour <= 15:
        start_datetime = start_datetime.replace(hour=16, minute=30)
        print('start_datetime is ', start_datetime)
    
    while not is_handelstag(start_datetime):
        print(f"{start_datetime} ist kein Handelstag, springe zum nächsten Tag.")
        start_datetime = start_datetime + timedelta(days=1)
        start_datetime = start_datetime.replace(hour=16, minute=30)  # Setze die Startzeit auf 16:30 des nächsten Handelstages
        print(f"{start_datetime}")    
    print(f"Prüfe Breakouts ab {start_datetime}")

    # Excel-Daten laden
    df = load_resistance_data()    
    
    #df['All Time High Date'] = pd.to_datetime(df['All Time High Date'], errors='coerce')
    # Filtere das DataFrame nach den angegebenen Kriterien
    #filtered_df = df[(df['All Time High Date'].dt.date < start_datetime.date()) & 
    #                (df['All Time High Date'].dt.date >= (start_datetime.date() - timedelta(days=10)))] 

    tickers = df['Symbol'].tolist()
    #print(tickers)
    breakout_tickers = []
    
# Haupt-Schleife
    max_attempts = 2  # Anzahl der Versuche bei JSONDecodeError
    delay = 70  # Wartezeit in Sekunden bei JSONDecodeError
    for ticker in tickers:
        for attempt in range(max_attempts):
            try:
                # Herunterladen der Daten mit yfinance
                start_date = (start_datetime - pd.Timedelta(days=100)).date()
                end_date = (start_datetime + pd.Timedelta(days=1)).date()

                tickers_df = yf.download(ticker, start=start_date, end=end_date, interval="1h")

                # Überprüfen, ob die Daten leer sind
                if tickers_df is None or tickers_df.empty:
                    print(f"Keine Daten für {ticker} im Zeitraum {start_date} bis {end_date}.")
                    save_ticker_no_data(ticker, start_date, end_date, "Keine Daten")
                    if attempt < max_attempts - 1:
                        print(f"Warte {delay} Sekunden, bevor der nächste Versuch gestartet wird.")
                        time.sleep(delay)
                        #tickers_df = yf.download(ticker, start=start_date, end=end_date, interval="1h")
                        continue  # Nächsten Versuch starten

                print(f" Ticker {ticker} Starttime: {start_datetime} Kapital: {cash}")
                break  # Schleife beenden, da Daten erfolgreich abgerufen wurden
            except Exception as e:
                print(f"Fehler beim Abrufen der Daten für {ticker} (Versuch {attempt + 1}): {e}")
                if attempt < max_attempts - 1:
                    print(f"Warte {delay} Sekunden, bevor der nächste Versuch gestartet wird.")
                    time.sleep(delay)
                continue
        else:
            print(f"Maximale Anzahl von Versuchen für {ticker} erreicht. Überspringe diesen Ticker.")
            continue

        # Zeitzonen-Konvertierung
        tickers_df.index = tickers_df.index.tz_convert("UTC")
        tickers_df.index = tickers_df.index.tz_convert(cet_tz)
        tickers_df.index = tickers_df.index.tz_localize(None)
        # Berechnung der EMAs
        tickers_df["EMA9"] = tickers_df["Close"].ewm(span=9, adjust=False).mean()
        tickers_df["EMA21"] = tickers_df["Close"].ewm(span=21, adjust=False).mean()
        tickers_df["EMA90"] = tickers_df["Close"].ewm(span=90, adjust=False).mean()
        
        # Filter auf den Startzeitpunkt
        filtered_1hdata = tickers_df[tickers_df.index <= start_datetime]
        
        # Überprüfen, ob gefilterte Daten vorhanden sind
        if filtered_1hdata.empty:
            print(f"Keine gefilterten Daten für {ticker} im Zeitraum {start_datetime}.")
            continue

        if len(filtered_1hdata) >= 50:
            avg_volume_50 = filtered_1hdata["Volume"].iloc[-50:].mean().iloc[-1].item()
        else:
            avg_volume_50 = None  # Standardwert setzen, wenn nicht genügend Daten vorhanden sind

        
            
        # Skalare Werte extrahieren
        last_close = filtered_1hdata["Close"].iloc[-1].item()
        
        last_open = filtered_1hdata["Open"].iloc[-1].item()
        last_high = filtered_1hdata["High"].iloc[-1].item()
        
        last_ema9 = filtered_1hdata["EMA9"].iloc[-1].item()
        last_ema21 = filtered_1hdata["EMA21"].iloc[-1].item()
        vorlast_close = filtered_1hdata["Close"].iloc[-2].item()
        vorlast_ema9 = filtered_1hdata["EMA9"].iloc[-2].item()
        vorlast_ema21 = filtered_1hdata["EMA21"].iloc[-2].item()
        last_volume = filtered_1hdata["Volume"].iloc[-1].item()
        
        
        # Breakout-Bedingungen überprüfen
        body = abs(last_close - last_open)
        
        upper_wick = last_high - last_close
        
        diff_percent = (last_close - last_ema9) / last_ema9 * 100

        if (
            ((vorlast_ema21 >= vorlast_ema9 and last_ema21 < last_ema9)
                or (vorlast_ema21 > vorlast_ema9 and last_ema21 <= last_ema9))
            and last_close > 30
            and last_volume > avg_volume_50
            and last_open <= vorlast_close * 1.0005
            and body >= upper_wick * 2
            and last_close > last_open):
            print(f"Breakout bei {ticker}: last_ema9 bei {last_ema9}, Last Close bei {last_close}, Last Open bei {last_open}")
            breakout_tickers.append(
                {
                    "Ticker": ticker,
                    "Last Close": last_close,
                    "Diff Percent": diff_percent,
                    "start_datetime": start_datetime,
                }
            )

    # Ergebnisse anzeigen
    print("Gefundene Breakout-Ticker:", breakout_tickers)
   
    
    # Wenn es Breakout-Ticker gibt, diese analysieren
    if breakout_tickers:
        print(breakout_tickers)
        breakout_df = pd.DataFrame(breakout_tickers)
        breakout_df = breakout_df.drop_duplicates()
        
        # Sicherstellen, dass 'Diff Percent' numerisch ist und keine NaN-Werte enthält
        breakout_df['Diff Percent'] = pd.to_numeric(breakout_df['Diff Percent'], errors='coerce')
        breakout_df = breakout_df.dropna(subset=['Diff Percent'])  # Entferne alle Zeilen mit NaN in 'Diff Percent'
        breakout_df.to_excel(output_excel_file2, index=False)
        
        # 1. Filter: 0.15% bis 0.80%
        filtered_df = breakout_df[(breakout_df['Diff Percent'] >= 0.15) & (breakout_df['Diff Percent'] <= 0.80)].sort_values('Diff Percent')
        if not filtered_df.empty:
            df_temp = filtered_df
            #print(df_temp)
            
            ticker_to_buy = df_temp.iloc[0]['Ticker']
            df_temp = filtered_df[filtered_df['Ticker'] == ticker_to_buy]
            
            
            #resistance_level = df_temp.iloc[0]['Resistance Level']
            last_close = df_temp.iloc[0]['Last Close']
            print(f"Kaufe Ticker {ticker_to_buy}, Diff Percent: {df_temp.iloc[0]['Diff Percent']:.2f}%, Schlusskurs: {last_close:.2f}")
            end_datetime = add_handelstage(start_datetime, 2)
            buy_ticker(ticker_to_buy,last_close, start_datetime, end_datetime,cash)
        else:
            # 2. Filter: >= 0.80% und <= 1.1%
            filtered_df = breakout_df[(breakout_df['Diff Percent'] >= 0.80) & (breakout_df['Diff Percent'] <= 1.1)].sort_values('Diff Percent')
            if not filtered_df.empty:
                df_temp = filtered_df
                #print(df_temp)
                ticker_to_buy = df_temp.iloc[0]['Ticker']
                
                filtered_breakout_df = breakout_df[breakout_df['Ticker'] == ticker_to_buy]
                
                #resistance_level = df_temp.iloc[0]['Resistance Level']
                last_close = df_temp.iloc[0]['Last Close']
                
                print(f"Kaufe Ticker {ticker_to_buy}, Diff Percent: {df_temp.iloc[0]['Diff Percent']:.2f}%, Schlusskurs: {last_close:.2f}")
                end_datetime = add_handelstage(start_datetime, 2)
                buy_ticker(ticker_to_buy,last_close, start_datetime, end_datetime,cash)
            else:
                # 3. Filter: 0.05% bis 0.15%, absteigend sortieren
                filtered_df = breakout_df[(breakout_df['Diff Percent'] >= 0.05) & (breakout_df['Diff Percent'] < 0.15)].sort_values('Diff Percent', ascending=False)
                if not filtered_df.empty:
                    df_temp = filtered_df
                    ticker_to_buy = df_temp.iloc[0]['Ticker']
                    #resistance_level = df_temp.iloc[0]['Resistance Level']
                    last_close = df_temp.iloc[0]['Last Close']
                    print(f"Kaufe Ticker {ticker_to_buy}, Diff Percent: {df_temp.iloc[0]['Diff Percent']:.2f}%, Schlusskurs: {last_close:.2f}")
                    end_datetime = add_handelstage(start_datetime, 2)
                    buy_ticker(ticker_to_buy,last_close , start_datetime, end_datetime, cash)
                else:
                    print("Keine passenden Ticker gefunden.")
                    # Wenn keine Breakout-Ticker gefunden wurden, erweitere den Zeitraum um eine Stunde und rufe die Funktion erneut auf
                    next_start_time = start_datetime + timedelta(hours=1)
                    if pd.Timestamp(next_start_time) < pd.Timestamp(global_end_datetime):
                        run_backtrader_strategy(next_start_time, cash)  # Nur wenn next_start_time vor dem Enddatum liegt
                    else:
                        print("Ende der Strategie erreicht.")
                        return
    else:
        print("Keine Breakout-Ticker gefunden.")
        # Wenn die Funktion erneut aufgerufen wird, das aktuelle Kapital weitergeben
        # Wenn keine Breakout-Ticker gefunden wurden, erweitere den Zeitraum um eine Stunde und rufe die Funktion erneut auf
        next_start_time = start_datetime + timedelta(hours=1)
        if pd.Timestamp(next_start_time) < pd.Timestamp(global_end_datetime):
            run_backtrader_strategy(next_start_time, cash)  # Nur wenn next_start_time vor dem Enddatum liegt
        else:
            print("Ende der Strategie erreicht.")
            return

    
    

    
########################################################################


def get_yfinance_minute_data(ticker, start_datetime, end_datetime):
    
    start_datetime= start_datetime - timedelta(hours=1)
    start_date = start_datetime.date()
    end_date = end_datetime.date() + pd.Timedelta(days=1)
    
    #print('end datum für T/S ',end_date)
    try:
        # Endzeitpunkt eine Stunde nach Startzeitpunkt      
        eurozone_tz = 'Europe/Berlin'
        # Daten aus yfinance abrufen
        tickers_df = yf.download(ticker, start=start_date, end=end_date, interval='1h')
        tickers_df.index = tickers_df.index.tz_convert('UTC')
        tickers_df.index = tickers_df.index.tz_convert(cet_tz)
        tickers_df.index = tickers_df.index.tz_localize(None)
        
        # Prüfen, ob Daten vorhanden sind
        if tickers_df.empty:
            print(f"Keine Daten für {ticker} im Zeitraum {start_datetime} gefunden.")
            return None
       
        # Filter auf den Startzeitpunkt (genaue Übereinstimmung)
        filtered_1hdata = tickers_df[(tickers_df.index > start_datetime) & (tickers_df.index <= end_datetime)]
        
        filtered_1hdata = filtered_1hdata[['Close', 'High', 'Low']]
        
        return filtered_1hdata
    
    except Exception as e:
        print(f"Fehler beim Abrufen der Daten für {ticker}: {e}")
        return None

#########################################################################
# Funktion, um den Kaufprozess durchzuführen



#######################################################################################
#######################################################################################
#######################################################################################


def buy_ticker(ticker, last_close, start_datetime, end_datetime, kapital):
  
    print(f"Starte Datenabruf für {ticker} von {start_datetime} bis {end_datetime}")
    data = get_yfinance_minute_data(ticker, start_datetime, end_datetime)
    
    # Überprüfen der Daten
    if data is None or data.empty:
        print(f"Keine 5-Minuten-Daten für {ticker} gefunden.")
        return
    
    # Auflösen des MultiIndex in einfache Spaltennamen
    if isinstance(data.columns, pd.MultiIndex):
         data.columns = data.columns.swaplevel(0, 1)  # Nur die erste Ebene (Price)
         data.columns = data.columns.droplevel(0)
         data.columns.name = None
    

    hold_period=2
    # Berechne die Take-Profit- und Stop-Loss-Preise
    buy_price = last_close
    stop_price = buy_price * stop_loss
    take_profit_price = buy_price * take_profit

    print(f"Kaufpreis: {buy_price}, Take-Profit: {take_profit_price}, Stop-Loss: {stop_price}")
    
    # Initialisiere Variablen
    start_datetime = data.index[0]  # Startzeitpunkt
    position_open = True  # Gibt an, ob die Position geöffnet ist
    result = None
    current_capital = kapital  # Verfügbares Kapital
    print(kapital)
    shares_bought = 0  # Gekaufte Menge

    
    # Kaufe Aktien
    shares_bought = int(current_capital // buy_price)  # Ganze Anzahl an Aktien
    if shares_bought == 0:
        print("Nicht genug Kapital für den Kauf von Aktien.")
        return {"Ergebnis": "Nicht genug Kapital", "Kapital": current_capital}
    
    invested_amount = shares_bought * buy_price
    current_capital -= invested_amount
    print(f"Aktien gekauft: {shares_bought}, Investiertes Kapital: {invested_amount}, Verbleibendes Kapital: {current_capital}")

    # Iteriere über die Daten
    for i, (index, row) in enumerate(data.iterrows()):
        current_high = row['High']
        current_low = row['Low']
        current_close = row['Close']

        print(f"Zeile {i}: High={current_high}, Low={current_low}, Close={current_close}, Zeitpunkt={index}")

        # Überprüfe Take-Profit-Bedingung
        if current_high >= take_profit_price and position_open:
            print(f"Take-Profit erreicht bei {current_high}")
            position_open = False
            sell_price = take_profit_price
            result = {'Ergebnis': 'Take-Profit', 'Preis': sell_price, 'Zeitpunkt': index}
            current_capital += shares_bought * sell_price
            break

        # Überprüfe Stop-Loss-Bedingung
        elif current_low <= stop_price and position_open:
            print(f"Stop-Loss erreicht bei {current_low}")
            position_open = False
            sell_price = stop_price
            result = {'Ergebnis': 'Stop-Loss', 'Preis': sell_price, 'Zeitpunkt': index}
            current_capital += shares_bought * sell_price
            print(current_capital)
            break

        # Überprüfe Haltezeit-Bedingung
        elif (index - start_datetime).days >= hold_period and position_open:
            print(f"Haltezeit überschritten. Verkaufe bei {current_close}")
            position_open = False
            sell_price = current_close
            result = {'Ergebnis': 'Haltezeit', 'Preis': sell_price, 'Zeitpunkt': index}
            current_capital += shares_bought * sell_price
            break

    # Falls keine der Bedingungen erfüllt wurde, verkaufe am Ende der Daten
    if position_open:
        final_close = data.iloc[-1]['Close']
        print(f"Verkauf zum Schlusskurs: {final_close}")
        result = {'Ergebnis': 'Ende der Daten', 'Preis': final_close, 'Zeitpunkt': data.index[-1]}
        current_capital += shares_bought * final_close

    print(f"Endkapital: {current_capital}")

    # Ergebnis in Excel speichern
    save_to_excel(output_excel_file,result,ticker, start_datetime, buy_price, shares_bought, kapital, current_capital)
    new_datetime = index + timedelta(hours=1)
    run_backtrader_strategy(new_datetime, current_capital)
    return result

def save_to_excel(output_excel_file, result,ticker,start_datetime,buy_price, shares_bought, invested_amount, current_capital):
    # Ergebnis-Daten in ein DataFrame umwandeln
    result_data = {
        "Ticker": ticker,
        "Kaufzeitpunkt": start_datetime,
        "Kaufpreis": buy_price,
        "Verkaufspreis": result['Preis'],
        "Verkaufszeitpunkt": result['Zeitpunkt'],
        "Ergebnis": result['Ergebnis'],
        "Investiertes Kapital": round(invested_amount,2),
        "Kapital": round(current_capital,2),
        "Menge": shares_bought,
    }
    result_df = pd.DataFrame([result_data])

    # Falls die Datei existiert, lade sie und füge neue Daten hinzu
    if os.path.exists(output_excel_file):
        existing_df = pd.read_excel(output_excel_file)
        result_df = pd.concat([existing_df, result_df], ignore_index=True)

    # Speichere die Datei
    result_df.to_excel(output_excel_file, index=False)
    print(f"Ergebnisse in Excel gespeichert: {output_excel_file}")
    




initial_cash = 10000 # Beispielhaftes Startkapital

start_datetime = '2024-01-02 16:30:00'
start_datetime = datetime.strptime(start_datetime, '%Y-%m-%d %H:%M:%S')
global_end_datetime = datetime.strptime('2024-10-31 22:00:00', '%Y-%m-%d %H:%M:%S')

run_backtrader_strategy(start_datetime,initial_cash)

