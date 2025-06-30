import { addUserValidation, loginUserValidation } from "./userValidation.js";
import { addProductValidation } from "./productValidation.js";
import {
  addOrderValidation,
  updateStatusOrderValidation,
} from "./orderValidation.js";

const validate = (schema) => (req, res, next) => {
  const { error, value } = schema.validate(req.body); 
  if (error) {
    const errorMessage = error.details
      .map((detail) => detail.message)
      .join(" ,");
    return res.status(400).json({ msg: errorMessage });
  }
  req.validatedBody = value; 
  next();
};
export {
  validate,
  addUserValidation,
  loginUserValidation,
  addProductValidation,
  addOrderValidation,
  updateStatusOrderValidation,
};