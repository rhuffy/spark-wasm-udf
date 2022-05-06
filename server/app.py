import os
from flask import Flask, render_template, flash, request, redirect, url_for
from flask_restful import Api, Resource, reqparse
from server.executor import execute, list_data_files, get_schema
from werkzeug.utils import secure_filename

UPLOAD_FOLDER = 'static/user_uploads'
ALLOWED_EXTENSIONS = {'csv'}

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

api = Api(app)

parser = reqparse.RequestParser()
parser.add_argument('task')


class Execute(Resource):
    def post(self):
        args = parser.parse_args()
        return {"resultPath": execute(args)}


class Files(Resource):
    def get(self):
        return {
            "names": list_data_files()
        }


class Columns(Resource):
    def get(self, file_name):
        return get_schema(file_name)


api.add_resource(Execute, '/execute')
api.add_resource(Execute, '/files')
api.add_resource(Execute, '/columns')


@app.route("/")
@app.route("/home")
@app.route("/index")
def index():
    return render_template("index.html")


@app.route("/editor")
def editor():
    return render_template("editor.html")


@app.route("/upload", methods=['GET', 'POST'])
def upload():
    if request.method == 'GET':
        return render_template("upload.html")
    elif request.method == 'POST':
        # check if the post request has the file part
        if 'file' not in request.files:
            flash('No file part')
            return redirect(request.url)
        file = request.files['file']
        # If the user does not select a file, the browser submits an
        # empty file without a filename.
        if file.filename == '':
            flash('No selected file')
            return redirect(request.url)
        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
            return render_template("upload.html", status='SUCCESS')
        return render_template("upload.html", status='ERROR')


def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS
