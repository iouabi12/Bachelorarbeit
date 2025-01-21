import backtrader as bt
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta, date
import pytz
import os
import time
# CET-Zeitzone definieren
cet_tz = pytz.timezone('Europe/Berlin')

# Pfad zur Excel-Liste mit den Widerstandsniveaus
excel_file_path = r"C:\Users\49163\Desktop\alles\Krypto\All time high\Bachelorarbeit\widerstandniveau\Widerstandsniveaus.xlsx"

# Pfad für das Excel-Output (Kapitalentwicklung und Ziele)
output_excel_file = r'C:\Users\49163\Desktop\alles\Krypto\All time High\Kapitalentwicklung_echtniveau_1%_2%.xlsx'
output_excel_file1 = r'C:\Users\49163\Desktop\alles\Krypto\All time High\filtered_df.xlsx'
output_excel_file2 = r'C:\Users\49163\Desktop\alles\Krypto\All time High\breakout_df.xlsx'

take_profit=1.02
stop_loss=0.99

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
    df = pd.read_excel(excel_file_path)
    return df




####################################################################################
#####################################################################################
#######################################################################################



# Backtrader-Strategie für 5-Minuten-Kerzen nach Breakout
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

def get_hourly_data_from_list(ticker, start_datetime):
    
    start_date = get_previous_handelstag(start_datetime)
    print(start_date)

    start_date = start_date.date()
    print('start_date', start_date)
    end_date = (start_datetime + pd.Timedelta(days=1)).date()
    print(end_date)
    
    
    try:
        # Endzeitpunkt eine Stunde nach Startzeitpunkt      
        
        # Daten aus yfinance abrufen
        tickers_df = yf.download(ticker, start=start_date, end=end_date, interval='1h')
        eurozone_tz = 'Europe/Berlin'
        tickers_df.index = tickers_df.index.tz_convert('UTC')
        tickers_df.index = tickers_df.index.tz_convert(eurozone_tz)
        tickers_df.index = tickers_df.index.tz_localize(None)

        # Prüfen, ob Daten vorhanden sind
        if tickers_df.empty:
            print(f"Keine Daten für {ticker} im Zeitraum {start_datetime} gefunden.")
            return None
        tickers_df.columns = tickers_df.columns.swaplevel(0, 1)  # Nur die erste Ebene (Price)
        tickers_df.columns = tickers_df.columns.droplevel(0)
        
        # Filter auf den Startzeitpunkt (genaue Übereinstimmung)
        filtered_1hdata = tickers_df[tickers_df.index <= start_datetime]
        #print(filtered_1hdata)

        if tickers_df.empty:
            print(f"Keine Daten für {ticker} im Zeitraum {start_datetime} gefunden.")
            return None
        # Umbenennen der Spalten für Konsistenz mit vorherigem Code
        tickers_df = tickers_df.rename(columns={
            'Open': 'Open',
            'Close': 'Close',
            'High': 'High',
            'Low': 'Low'
        })
        return filtered_1hdata
    
    except Exception as e:
        print(f"Fehler beim Abrufen der Daten für {ticker}: {e}")
        return None
############################################################################

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

#######################################################################
#######################################################################



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
    print(start_datetime)
# Lege eine Liste zum Speichern der Breakout-Ticker an
    breakout_tickers = []

    # Startdatum um 3 Stunden reduzieren
    start_datetime_minus_3h = start_datetime - timedelta(hours=3)  #14:00
    df['All Time High Date'] = pd.to_datetime(df['All Time High Date'], errors='coerce')
    df['Level_datetime'] = pd.to_datetime(df['Level_datetime'], errors='coerce')  # Konvertierung mit Fehlerbehandlung
    startdate = (start_datetime - timedelta(days=7)).date()

    # Filtere das DataFrame nach den angegebenen Kriterien
    filtered_df = df[
        (df['All Time High Date'].dt.date < start_datetime.date()) &
        (df['All Time High Date'].dt.date >= startdate)
    ]

    filtered_df = filtered_df[filtered_df['Level_datetime'] < start_datetime_minus_3h]
    #print(filtered_df)
    # Gruppiere nach Ticker und finde das maximale Resistance Level für jeden Ticker
    grouped_df = filtered_df.groupby('Ticker').agg({'Resistance Level': 'max'}).reset_index()
    grouped_df = pd.merge(grouped_df, 
                      filtered_df[['Ticker', 'Resistance Level','All Time High', 'All Time High Date', 'Level_datetime']], 
                      on=['Ticker', 'Resistance Level'], 
                      how='left')
    max_attempts = 2  # Anzahl der Versuche bei JSONDecodeError
    delay = 70  # Wartezeit in Sekunden bei JSONDecodeError
    for index, row in grouped_df.iterrows():

        ticker = row['Ticker']
        resistance_level = row['Resistance Level']
        all_time_high = row['All Time High']
        all_time_high_datetime = row['All Time High Date']
        level_datetime = row['Level_datetime']
        for attempt in range(max_attempts):
        # Hole die 1-Stunden-Daten
            try:
                print(start_datetime - timedelta(hours=1))
                hourly_data = get_hourly_data_from_list(ticker, start_datetime - timedelta(hours=1))
                print(hourly_data)
                
                # Überprüfen, ob Daten vorhanden sind
                if hourly_data is None or hourly_data.empty:
                    if attempt < max_attempts - 1:
                        print(f"Warte {delay} Sekunden, bevor der nächste Versuch gestartet wird.")
                        time.sleep(delay)
                        #tickers_df = yf.download(ticker, start=start_date, end=end_date, interval="1h")
                        continue  # Nächsten Versuch starten
                print(f" Ticker {ticker} Starttime: {start_datetime} Kapital: {cash}")
                break  # Schleife beenden, da Daten erfolgreich abgerufen wurden
            except Exception as e:
                print(f"Fehler beim Abrufen der Daten für {ticker}: {e}")
                continue
        else:
            print(f"Maximale Anzahl von Versuchen für {ticker} erreicht. Überspringe diesen Ticker.")
            continue
            # Sicherstellen, dass der Index ein DatetimeIndex ist
        if not isinstance(hourly_data.index, pd.DatetimeIndex):
            hourly_data.index = pd.to_datetime(hourly_data.index)
        

        # Holen des letzten Schlusskurses
        last_close = hourly_data['Close'].iloc[-1].item() 
        
        last_high = hourly_data["High"].iloc[-1].item()
        last_open = hourly_data['Open'].iloc[-1].item() 
        print(last_close)
        print(last_high)
        body = abs(last_close - last_open)
        print(body)
        vorlast_close = hourly_data['Close'].iloc[-2].item()  if len(hourly_data) > 1 else None
        vorlast_High = hourly_data['High'].iloc[-2].item() if len(hourly_data) > 1 else None
        vorvorlast_High = hourly_data['High'].iloc[-3].item() if len(hourly_data) > 2 else None
        vorvorvorlast_High = hourly_data['High'].iloc[-4].item() if len(hourly_data) > 3 else None
        vorvorvorvorlast_High = hourly_data['High'].iloc[-5].item() if len(hourly_data) > 4 else None
        
        
        upper_wick = last_high - last_close
        previous_high_max = hourly_data['High'].iloc[:1].max().item()
        diff_percent = (last_close - resistance_level) / resistance_level * 100
        # Prüfen der Bedingungen für Breakout
        if (last_close > resistance_level and
            all_time_high < resistance_level and
            #all_time_high_datetime < level_datetime and
            last_close > vorlast_High if vorlast_High is not None else False and
            last_close > vorvorlast_High if vorvorlast_High is not None else False and
            last_close > vorvorvorlast_High if vorvorvorlast_High is not None else False and
            last_close > vorvorvorvorlast_High if vorvorvorvorlast_High is not None else False and
            last_open < resistance_level and
            vorlast_close is not None and
            last_open <= vorlast_close * 1.0005 and  # Letztes Open <= 0.05% über dem vorletzten Close
            body >= upper_wick * 2 and  # Körper >= 2x obere Wick
            last_close > last_open and # Schlusskurs größer als Open
            last_close > previous_high_max * 1.0005):  # last_close über allen vorherigen High-Werten außer dem letzten

            print(f"Breakout bei {ticker}: Widerstand bei {resistance_level}, All-Time-High bei {all_time_high}. Last Close bei {last_close}, Last Open bei {last_open}")
            
            breakout_tickers.append({
                'Ticker': ticker,
                'All Time High': all_time_high,
                'Resistance Level': resistance_level,
                'Last Close': last_close,
                'Diff Percent' : diff_percent,
                'All Time High Date': all_time_high_datetime,
                'Level_datetime': level_datetime,
                'start_datetime': start_datetime
            })

        

    # Wenn es Breakout-Ticker gibt, diese analysieren
    if breakout_tickers:
        breakout_df = pd.DataFrame(breakout_tickers)
        breakout_df = breakout_df.drop_duplicates()
        
        # Sicherstellen, dass 'Diff Percent' numerisch ist und keine NaN-Werte enthält
        breakout_df['Diff Percent'] = pd.to_numeric(breakout_df['Diff Percent'], errors='coerce')
        breakout_df = breakout_df.dropna(subset=['Diff Percent'])  # Entferne alle Zeilen mit NaN in 'Diff Percent'
        #breakout_df.to_excel(output_excel_file2, index=False)
        
        # 1. Filter: 0.15% bis 0.80%
        filtered_df = breakout_df[(breakout_df['Diff Percent'] >= 0.15) & (breakout_df['Diff Percent'] <= 0.80)].sort_values('Diff Percent')
        if not filtered_df.empty:
            df_temp = filtered_df
            print(df_temp)
            
            ticker_to_buy = df_temp.iloc[0]['Ticker']
            df_temp = filtered_df[filtered_df['Ticker'] == ticker_to_buy]
            
            print(ticker_to_buy)
            resistance_level = df_temp.iloc[0]['Resistance Level']
            last_close = df_temp.iloc[0]['Last Close']
            print(f"Kaufe Ticker {ticker_to_buy}, Diff Percent: {df_temp.iloc[0]['Diff Percent']:.2f}%, Schlusskurs: {last_close:.2f}")
            end_datetime = add_handelstage(start_datetime, 2)
            buy_ticker(ticker_to_buy,last_close, start_datetime, end_datetime,cash)
        else:
            # 2. Filter: >= 0.80% und <= 1.1%
            filtered_df = breakout_df[(breakout_df['Diff Percent'] >= 0.80) & (breakout_df['Diff Percent'] <= 1.1)].sort_values('Diff Percent')
            if not filtered_df.empty:
                df_temp = filtered_df
                print(df_temp)
                ticker_to_buy = df_temp.iloc[0]['Ticker']
                print(ticker_to_buy)
                filtered_breakout_df = breakout_df[breakout_df['Ticker'] == ticker_to_buy]
                print(filtered_breakout_df)
                resistance_level = df_temp.iloc[0]['Resistance Level']
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
                    resistance_level = df_temp.iloc[0]['Resistance Level']
                    last_close = df_temp.iloc[0]['Last Close']
                    print(f"Kaufe Ticker {ticker_to_buy}, Diff Percent: {df_temp.iloc[0]['Diff Percent']:.2f}%, Schlusskurs: {last_close:.2f}")
                    end_datetime = add_handelstage(start_datetime, 2)
                    buy_ticker(ticker_to_buy,last_close, start_datetime, end_datetime,cash)
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







initial_cash = 10000


start_datetime = '2024-01-02 16:30:00'
start_datetime = datetime.strptime(start_datetime, '%Y-%m-%d %H:%M:%S')
global_end_datetime = datetime.strptime('2024-10-31 22:00:00', '%Y-%m-%d %H:%M:%S')

run_backtrader_strategy(start_datetime,initial_cash)

import winsound
try:
    winsound.Beep(1000, 500)
except:
    pass