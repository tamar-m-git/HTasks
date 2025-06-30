import express from "express";
import {
  getSuppliers


} from "../controllers/supplierController.js";
import {
  authenticateToken,
  authorizeOwner
} from "../middleware/auth.js";

const SupplierRouter = express.Router();

SupplierRouter.get("/getSuppliers", authenticateToken, authorizeOwner, getSuppliers);


export default SupplierRouter;