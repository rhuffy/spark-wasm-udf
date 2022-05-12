from flask import Flask
from flask_restful import Api, Resource, reqparse
from flask_cors import CORS
from executor import execute, list_data_files, get_schema, get_result_if_exists

UPLOAD_FOLDER = 'static/user_uploads'
ALLOWED_EXTENSIONS = {'csv'}

app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": "*"}})
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

api = Api(app)

parser = reqparse.RequestParser()
parser.add_argument('program', type=str)
parser.add_argument('data', type=str)
parser.add_argument('operation', type=str)
parser.add_argument('function_name', type=str)
parser.add_argument('input_column_names', type=list, location='json')
parser.add_argument('output_column_name', type=str)
parser.add_argument('output_column_type', type=str)


class Execute(Resource):
    def post(self):
        args = parser.parse_args()
        return {"id": execute(args)}


class Files(Resource):
    def get(self):
        return {
            "names": list_data_files()
        }


class Schema(Resource):
    def get(self, filename):
        print(filename)
        return {"schema": get_schema(filename)}


class Result(Resource):
    def get(self, id):
        return {"data": get_result_if_exists(id)}


api.add_resource(Execute, '/api/execute')
api.add_resource(Files, '/api/files')
api.add_resource(Schema, '/api/files/<string:filename>/schema')
api.add_resource(Result, '/api/execute/<string:id>')
