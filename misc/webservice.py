from __future__ import annotations

import os
from flask import Flask, request, jsonify, send_file
from werkzeug.utils import secure_filename
from database import get_progress

app = Flask(__name__)
INPUT_DIRECTORY = os.getenv('OCR_INPUT_DIRECTORY', '/input')
OUTPUT_DIRECTORY = os.getenv('OCR_OUTPUT_DIRECTORY', '/output')
app.config['UPLOAD_FOLDER'] = INPUT_DIRECTORY
app.config['DOWNLOAD_FOLDER'] = OUTPUT_DIRECTORY


@app.route('/upload', methods=['POST'])
def upload():
    if 'file' not in request.files:
        res = jsonify({'error': 'No file was uploaded'})
        res.headers.add('Access-Control-Allow-Origin', '*')
        return res
    file = request.files['file']
    if file.filename == '':
        res = jsonify({'error': 'No file was selected'})
        res.headers.add('Access-Control-Allow-Origin', '*')
        return res

    filename = secure_filename(file.filename)

    save_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)

    file.save(save_path)

    res = jsonify({'message': 'Upload successful'})
    res.headers.add('Access-Control-Allow-Origin', '*')
    return res


@app.route('/check', methods=['GET'])
def check():
    for unwanted_file in os.listdir(app.config['DOWNLOAD_FOLDER']):
        if unwanted_file.endswith(".pdf") or unwanted_file.endswith(".PDF"):
            pass
        else:
            os.remove(os.path.join(app.config['DOWNLOAD_FOLDER'], unwanted_file))
    files = os.listdir(app.config['DOWNLOAD_FOLDER'])
    filenames = ','.join(files)
    processing_files = ','.join(get_progress("pending"))
    if filenames == "":
        filenames = processing_files
    elif processing_files != "" and filenames != "":
        filenames += ',' + processing_files
    if len(filenames) == 0:
        res = jsonify({'message': 'FILES_NOT_READY'})
    else:
        res = jsonify({'message': filenames})

    res.headers.add('Access-Control-Allow-Origin', '*')
    return res


@app.route('/download', methods=['GET'])
def download():
    file_name = request.args.get('filename')
    path = os.path.join(app.config['DOWNLOAD_FOLDER'], file_name)
    res = send_file(path, as_attachment=True)
    res.headers.add('Access-Control-Allow-Origin', '*')
    os.remove(path)
    return res


if __name__ == "__main__":
    app.run(host='0.0.0.0', debug=True, port=5000)
