import Joi from 'joi'

export const addUserValidation=Joi.object({
  companyName: Joi.string().min(3).max(30).pattern(/^[A-Za-z\u0590-\u05FF ]+$/).required(),
  phoneNumber:  Joi.string().pattern(/^0\d{8,9}$/).min(9).max(100).required(),
  representativeName: Joi.string().min(2).max(30).required(),
   password: Joi.string()
    .min(8)
    .max(30)
    .pattern(/^[A-Za-z0-9!@#$%^&*()_+\-=\[\]{};':"\\|,.<>\/?]+$/)
    .required()
})

export const loginUserValidation=Joi.object({
  companyName: Joi.string().min(3).max(30).pattern(/^[A-Za-z\u0590-\u05FF ]+$/).required(),
   password: Joi.string()
    .min(8)
    .max(30)
    .pattern(/^[A-Za-z0-9!@#$%^&*()_+\-=\[\]{};':"\\|,.<>\/?]+$/)
    .required()
})
