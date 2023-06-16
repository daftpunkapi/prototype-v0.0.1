from flask import Flask, request
from os import environ

app = Flask(__name__)

@app.route('/customerinput', methods=['POST'])
def customer_input():
    body_params = request.json
    customer_id = body_params["customer_id"]
    zone = body_params["zone"]
    print("Customer ID:", customer_id)
    print("Zone:", zone)

    # Internal API GET request for restuarant live pending orders
    
    # Internal API GET request for user 30d history

    # After both satisfy, then ML inference (gRPC / GET)
    # return 'top 5 restuarants'
    return  "OK"

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=environ.get("PORT", 3000))