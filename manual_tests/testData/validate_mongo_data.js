use application_examples;

var ccCount = db.getCollection('creditCards').count();
if (ccCount !== 1001) {
    print('Credit Card count is not correct!');
}

var customerCount = db.getCollection('customers').count();
if (customerCount !== 1001) {
    print('Customer count is not correct!');
}

var orderCount = db.getCollection('orders').count();
if (orderCount !== 2001) {
    print('Order count is not correct!');
}

var productCount = db.getCollection('products').count();
if (productCount !== 868) {
    print('Product count is not correct!');
}
