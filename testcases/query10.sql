SELECT CUSTOMER.cid,CUSTOMER.gender,CUSTOMER.firstname,CUSTOMER.lastname,CUSTOMER.address,CART.cartid,CART.cid,CART.status,CART.remarks,CARTDETAILS.iid,CARTDETAILS.cartid,CARTDETAILS.qty,CARTDETAILS.remarks,BILL.billid,BILL.iid,BILL.amount,BILL.remarks
FROM CUSTOMER,CART,CARTDETAILS,BILL
WHERE CUSTOMER.cid=CART.cid,CART.cartid=CARTDETAILS.cartid,CARTDETAILS.iid=BILL.iid,BILL.amount<"1000",BILL.amount>"500"
