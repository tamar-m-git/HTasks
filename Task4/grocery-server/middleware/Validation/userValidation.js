import Joi from 'joi'

export const addUserValidation=Joi.object({
  userName: Joi.string().min(3).max(30).required(), 
  password: Joi.string()
    .min(8)
    .max(30)
    .pattern(/^[A-Za-z0-9!@#$%^&*()_+\-=\[\]{};':"\\|,.<>/?]+$/)
    .required(),
  role: Joi.string().valid("owner", "supplier").required(), 
  companyName: Joi.string().min(3).max(30).pattern(/^[A-Za-z\u0590-\u05FF ]+$/).when('role', {
    is: 'supplier', 
    then: Joi.required(), 
    otherwise: Joi.optional() 
  }),
  phoneNumber:  Joi.string().pattern(/^0\d{8,9}$/).min(9).max(100).when('role', {
    is: 'supplier', 
    then: Joi.required(), 
    otherwise: Joi.optional()
  }),
  representativeName: Joi.string().min(2).max(30).when('role', {
    is: 'supplier', 
    then: Joi.required(), 
    otherwise: Joi.optional() 
  }),
});

export const loginUserValidation=Joi.object({
  userName: Joi.string().min(3).max(30).required(), 
   password: Joi.string()
    .min(8)
    .max(30)
    .pattern(/^[A-Za-z0-9!@#$%^&*()_+\-=\[\]{};':"\\|,.<>/?]+$/)
    .required()
});