SELECT CART.cartid,CART.cid,CART.status,CART.remarks,CARTDETAILS.iid,CARTDETAILS.cartid,CARTDETAILS.qty,CARTDETAILS.remarks
FROM CART,CARTDETAILS
WHERE CART.cartid=CARTDETAILS.cartid
