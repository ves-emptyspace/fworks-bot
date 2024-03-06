import logging
import os
from io import BytesIO, BufferedIOBase
import json
from functools import wraps
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseUpload
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd

from telegram import ForceReply, Update
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters


TOKEN = os.environ.get("TOKEN")
CREDENTIALS = json.load(open(r"./etc/secrets/CREDENTIALS.json"))
FOLDER_ID = os.environ.get("FOLDER_ID")
SCOPE = ['https://www.googleapis.com/auth/drive']
LIST_OF_ADMINS = [int(admin) for admin in os.environ.get("ADMINS").split(', ')] 


def restricted(func):
    @wraps(func)
    async def wrapped(update, context, *args, **kwargs):
        user_id = update.effective_user.id
        if user_id not in LIST_OF_ADMINS:
            print(f"Unauthorized access denied for {user_id}.")
            return
        return await func(update, context, *args, **kwargs)
    return wrapped


class GoogleDrive:
  def __init__(self, credentials):
    credentials = (ServiceAccountCredentials
                        .from_json_keyfile_dict(credentials, 
                                                scopes=['https://www.googleapis.com/auth/drive']))
    self.service = build('drive', 'v3', credentials=credentials)
    self.df_files = pd.DataFrame()
    files = (self.service
                 .files()
                 .list(pageSize=1000, 
                       fields="nextPageToken, files(id, name, webViewLink, mimeType)").execute())
    self.df_files = pd.DataFrame(files.get('files', []))
    self.link = self.df_files.query('name == "fworks"').webViewLink.values[0]
  
  def upload(self, file_bytesio, file_name):
    # Check file existing  
    file_id = self.df_files.query('name == @file_name').id.to_list()
    if len(file_id) >= 1:
      file_id = file_id[0]
  
    # Proccess pdf file to upload
    media = MediaIoBaseUpload(file_bytesio,
                            mimetype='application/pdf')
    
    returned_fields = "id, name, webViewLink"
    # Update or create file
    if file_id:
      file = self.service.files().update(fileId=file_id, media_body=media, 
                                         fields=returned_fields).execute()
    else:
      file_metadata = {'name': file_name, 'parents': [FOLDER_ID]}
      file = self.service.files().create(body=file_metadata, media_body=media,
                                    fields=returned_fields, supportsTeamDrives=True).execute()
    return file


# Enable logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
# set higher logging level for httpx to avoid all GET and POST requests being logged
logging.getLogger("httpx").setLevel(logging.WARNING)

logger = logging.getLogger(__name__)


# Define a few command handlers. These usually take the two arguments update and
# context.
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
  """Send a message when the command /start is issued."""
  user = update.effective_user
  text = json.dumps(update.to_dict(), indent=2)
  await context.bot.send_message(chat_id=update.effective_chat.id, text=text)
  # await update.message.reply_html(
  #     rf"Hi {user.mention_html()}!",
  #     reply_markup=ForceReply(selective=True),
  # )


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
  """Send a message when the command /help is issued."""
  await update.message.reply_text("Help!")

@restricted
async def link_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
  """Send a message when the command /link is issued."""
  gd = GoogleDrive(CREDENTIALS)
  respond_message = f'Link to folder: {gd.link}'
  await update.message.reply_text(respond_message)
  

@restricted
async def downloader(update, context):
  file = await context.bot.get_file(update.message.document)
  file_name = update.message.document.file_name
  buf = BytesIO()
  await file.download_to_memory(buf)
  gd = GoogleDrive(CREDENTIALS)
  respond = gd.upload(buf, file_name)
  respond_message = f'File {respond["name"]} uploaded to Google Drive. \n' \
                    f'Link to file: {respond["webViewLink"]} \n' \
                    f'Link to folder: {gd.link}'
  await update.message.reply_text(respond_message)

def main() -> None:
  """Start the bot."""
  # Create the Application and pass it your bot's token.
  application = Application.builder().token(TOKEN).build()

  # on different commands - answer in Telegram
  application.add_handler(CommandHandler("start", start))
  application.add_handler(CommandHandler("help", help_command))
  application.add_handler(CommandHandler("link", link_command))

  # on non command i.e message - echo the message on Telegram
  application.add_handler(MessageHandler(filters.Document.ALL, downloader))

  # Run the bot until the user presses Ctrl-C
  application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
  main()
