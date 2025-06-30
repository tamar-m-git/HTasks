import express from "express";
import {
  loginUser,
  registerUser

} from "../controllers/userController.js";
//import {
//  authenticateToken,
 // authorizeOwner,authorizeOwnerOrSupplier
//} from "../middleware/auth.js";

import { addUserValidation,loginUserValidation } from "../middleware/Validation/userValidation.js";
import { validate } from "../middleware/Validation/validateMain.js";
const UserRouter = express.Router();


UserRouter.post("/register",validate(addUserValidation),registerUser);
UserRouter.post("/login",validate(loginUserValidation),loginUser);


export default UserRouter;