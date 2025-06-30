import express from "express";
import {completeOrder,confirmOrder,createOrder,getAllOrders,getOrdersBySupplier}
   from "../controllers/orderController.js";
import {
  authenticateToken,
  authorizeOwner,authorizeOwnerOrSupplier,authorizeSupplier
} from "../middleware/auth.js";

import { updateStatusOrderValidation,addOrderValidation } from "../middleware/Validation/orderValidation.js";
import { validate } from "../middleware/Validation/validateMain.js";
const OrderRouter = express.Router();


OrderRouter.post("/createOrder",validate(addOrderValidation),authenticateToken,authorizeOwner,createOrder);
OrderRouter.get("/getAllOrders",authenticateToken,authorizeOwner,getAllOrders);
OrderRouter.get("/getOrdersBySupplier",authenticateToken,authorizeSupplier,getOrdersBySupplier);
OrderRouter.put("/confirm/:id", authenticateToken, authorizeSupplier, confirmOrder);
OrderRouter.put("/complete/:id",authenticateToken, authorizeOwner, completeOrder);


export default OrderRouter;