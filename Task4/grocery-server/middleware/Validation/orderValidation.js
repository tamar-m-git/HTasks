import Joi from 'joi'

export const updateStatusOrderValidation=Joi.object({
  status: Joi.string().valid("created", "in progress", "completed").required()
});
export const addOrderValidation = Joi.object({
  supplierId: Joi.string().hex().length(24).required(),
  listItems: Joi.array().items(
    Joi.object({
      productId: Joi.string().hex().length(24).pattern(/^[A-Za-z\u0590-\u05FF ]+$/).required(),
      quantity: Joi.number().integer().min(1).required()
    })
  ).min(1).required()
});
