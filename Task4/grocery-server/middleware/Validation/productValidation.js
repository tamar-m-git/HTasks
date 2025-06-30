import Joi from 'joi'

export const addProductValidation=Joi.object({
    name: Joi.string().min(2).max(30).pattern(/^[A-Za-z\u0590-\u05FF ]+$/).required(),
    price: Joi.number().min(0).required(),
    minQuantity:Joi.number().integer().min(0).required(),
     supplierId: Joi.string().hex().length(24).required()
})