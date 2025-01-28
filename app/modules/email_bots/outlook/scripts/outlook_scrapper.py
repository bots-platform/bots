import win32com.client
import re
import pandas as pd
from datetime import datetime
import os


def scrape_from_outlook():  
    outlook = win32com.client.Dispatch("Outlook.Application").GetNamespace("MAPI")
    inbox = outlook.Folders("Soporte Ministerio Publico").Folders("Bandeja de entrada")

    ticket_pattern = re.compile(r'\bticket\b', re.IGNORECASE)
    #sent_date_pattern = re.compile(r'(?:Enviado el:|Sent:)\s*(.+?)\s(\d{1,2}:\d{2}\s*(?:AM|PM|am|pm)?)', re.IGNORECASE)

    email_data = []
    count = 0

    print("\nProcesando mensajes...\n")

    for index, message in enumerate(inbox.Items, start=1):
        try:
            # Verificar si el elemento es un correo (MailItem)
            if message.Class == 43:  # 43 es el código para MailItem
                sender = message.SenderName if hasattr(message, 'SenderName') else { "Sin remitente"}
                subject = message.Subject if hasattr(message, 'Subject') else "Sin asunto"

                try:
                    body = message.Body
                except Exception:
                    body = "Error al acceder al cuerpo del mensaje"

                ticket_match = ticket_pattern.search(body)
                if ticket_match:
                    ticket_number = "ok"
                else:
                    ticket_number = "No encontrado"

                # sent_date_match = sent_date_pattern.findall(body)
                # if sent_date_match:
                #     sent_date, sent_time = sent_date_match[0]
                #     sent_time = sent_time.strip()
                # else:
                #     sent_date = "No encontrado"
                #     sent_time = "No encontrado"

                data = {
                    'Fecha': message.ReceivedTime.strftime('%Y-%m-%d'),
                    'Hora': message.ReceivedTime.strftime('%H:%M:%S'),
                    'Remitente': sender,
                    'Asunto': subject,
                    'Ticket': ticket_number,
                    # 'Enviado/Sent': f"{sent_date} {sent_time}",
                    # 'Enviado/Sent Fecha': sent_date,
                    # 'Enviado/Sent Hora': sent_time,
                    'Cuerpo Completo': body
                }

                email_data.append(data)
                count += 1

                print(f"Procesado mensaje {index}: '{subject}' de '{sender}'")

        except Exception as e:
            print(f"\nError en mensaje {index}")
            print(f"Detalles del error: {str(e)}\n")
            continue

    if email_data:
        df = pd.DataFrame(email_data)

        if not os.path.exists('media'):
            os.makedirs('media')

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = f"media/email/downloads/emails_tickets_{timestamp}.xlsx"
        df.to_excel(output_file, index=False, engine='openpyxl')

        print(f"\nArchivo guardado como: {output_file}")
        print(f"Total mensajes procesados: {count}")
    else:
        print("No se encontraron mensajes con tickets.")

    return output_file
