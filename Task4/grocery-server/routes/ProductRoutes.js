import express from "express";
import {
  getProductBySupplier,
  createProduct

} from "../controllers/productController.js";
import {
  authenticateToken,
  authorizeSupplier,authorizeOwnerOrSupplier
} from "../middleware/auth.js";

import { addProductValidation } from "../middleware/Validation/productValidation.js";
import { validate } from "../middleware/Validation/validateMain.js";
const ProductRouter = express.Router();


ProductRouter.post("/createProduct",validate(addProductValidation),authenticateToken,authorizeSupplier,createProduct);
ProductRouter.get("/getProductBySupplier", authenticateToken,authorizeOwnerOrSupplier,getProductBySupplier);


export default ProductRouter;