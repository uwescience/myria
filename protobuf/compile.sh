protoc *.proto --java_out=./java --python_out=./python --cpp_out=./cpp
echo "To run the datalog web application, you must replace _google.protobuf_ with _protobuf_ in logical_ra_pb2.py!"
echo "Use this command:"
echo "sed -i \"\" 's/google\.protobuf/protobuf/g' python/logical_ra_pb2.py"
