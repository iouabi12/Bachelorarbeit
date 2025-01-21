from alpaca_trade_api.rest import REST

# API-Schlüssel und geheimer Schlüssel für Alpaca
api_key = 'PKPAHSVJ03KSEAVKJ3DC'
api_secret = 'sexBtgi5wcqa5nL5bJSADenO30gZ7nJvLexdDzLO'
base_url = 'https://paper-api.alpaca.markets'

# Alpaca API-Client initialisieren
api = REST(api_key, api_secret, base_url, api_version='v2')

def cancel_all_open_orders():
    try:
        # Alle offenen Orders abrufen
        open_orders = api.list_orders(status='open')

        if not open_orders:
            print("Keine offenen Orders gefunden.")
            return

        # Schleife über alle offenen Orders und diese stornieren
        for order in open_orders:
            print(f"Storniere Order {order.id} für Symbol {order.symbol} mit Status {order.status}.")
            api.cancel_order(order.id)  # Order stornieren
        
        print("Alle offenen Orders wurden storniert.")
    
    except Exception as e:
        print(f"Fehler beim Stornieren der Orders: {e}")

# Funktion ausführen
cancel_all_open_orders()
