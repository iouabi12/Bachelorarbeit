from alpaca_trade_api.rest import REST

# API-Schlüssel und geheimer Schlüssel für Alpaca
api_key = 'PKPAHSVJ03KSEAVKJ3DC'
api_secret = 'sexBtgi5wcqa5nL5bJSADenO30gZ7nJvLexdDzLO'
base_url = 'https://paper-api.alpaca.markets'

# Alpaca API-Client initialisieren
api = REST(api_key, api_secret, base_url, api_version='v2')

def close_all_positions():
    try:
        # Alle offenen Positionen abrufen
        positions = api.list_positions()

        if not positions:
            print("Keine offenen Positionen gefunden.")
            return

        # Schleife über alle Positionen und diese schließen
        for position in positions:
            symbol = position.symbol
            qty = abs(int(position.qty))  # Absolute Menge abrufen
            print(qty)
            side = 'sell' if int(position.qty) > 0 else 'buy'  # Verkaufs- oder Kauforder für die Position
            
            print(f"Schließe Position für {symbol} mit {qty} Einheiten ({'Short' if side == 'buy' else 'sell'}).")

            # Verkaufsorder erstellen
            api.submit_order(
                symbol=symbol,
                qty=qty,
                side=side,
                type='market',  # Marktorder
                time_in_force='day'  # Gültig bis zum Abbruch
            )
        
        print("Alle Positionen wurden geschlossen.")
    
    except Exception as e:
        print(f"Fehler beim Schließen der Positionen: {e}")

# Funktion ausführen
close_all_positions()
