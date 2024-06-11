#This is program to transcript audio file to text using AssemblyAI api.
#First signup on AssemblyAI to get free api key.

import requests
from api_secret import API_KEY_ASSEMBLYAI
import sys
import time

#audio_upload
header = {'authorization' : API_KEY_ASSEMBLYAI}
upload_endpoint = "https://api.assemblyai.com/v2/upload"
transcript_endpoint = "https://api.assemblyai.com/v2/transcript"
filename = sys.argv[1]

def upload(filename):
    print('Audio file upload started')
    def read_file(filename, chunk_size=5242880):
        with open(filename, 'rb') as _file:
            while True:
                data = _file.read(chunk_size)
                if not data:
                    break
                yield data
    
    #print(upload_endpoint)
    #print(header)

    upload_response = requests.post(upload_endpoint, headers=header, data= read_file(filename))
    #print(upload_response.json())
    audio_url = upload_response.json()['upload_url']
    print('File uploaded successfully')
    return audio_url

#transcribe
def transcribe(audio_url):
    transcript_request= {"audio_url" : audio_url}
    transcript_reponse = requests.post(transcript_endpoint, json=transcript_request, headers=header)
    job_id = transcript_reponse.json()['id']
    return job_id



#poll
def poll(transcript_id):
    polling_endpoint = transcript_endpoint + '/' + transcript_id
    polling_resonse = requests.get(polling_endpoint,headers=header)
    return polling_resonse.json()

def get_transcription_result_url(audio_url):
        print('Transcription started...')
        transcript_id = transcribe(audio_url)
        while True:
            data = poll(transcript_id)
            if data['status']=='completed':
                return data,None
            elif data['status']=='error':
                return data,data['error']
            print('Waiting')
            time.sleep(10)

def save_transcript(audio_url):
    #save transcript
    data, error = get_transcription_result_url(audio_url)
        
    #print(data)

    if data:
        text_filename = filename+'.txt'
        with open(text_filename,'w') as f:
            f.write(data['text'])
        print('Transcript saved')
    elif error:
        print('Error:' + error)

audio_url = upload(filename)
save_transcript(audio_url)