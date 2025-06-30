import Order from '../models/Order.js';
import Product from '../models/Product.js';

// view orders by supplier
export const getOrdersBySupplier = async (req, res) => {
    try {
        const orders = await Order.find({ supplierId: req.user.id }) //  מחפש רק הזמנות של הספק המחובר למערכת כרגע
        res.status(200).json(orders); 
    } catch (error) {
        res.status(500).json({ message: error.message });
    }
};


//view all orders by store owner
export const getAllOrders = async (req, res) => {
    try {
        const orders = await Order.find().populate('supplierId', 'companyName representativeName'); // מחפש את כל ההזמנות וממלא את פרטי הספק
        if (!orders || orders.length === 0) {
            return res.status(404).json({ message: 'No orders found.' });
        }
        res.status(200).json(orders);
    } catch (error) {
        res.status(500).json({ message: error.message });
    }
};

// create a new order
export const createOrder = async (req, res) => {
    const { listItems } = req.body;
    if (!listItems || listItems.length === 0) {
        return res.status(400).json({ message: 'Order must contain at least one item.' });
    }

    try {
        // בדיקה אם כל המוצרים קיימים במלאי
        for (const item of listItems) {
            const product = await Product.findById(item.productId);
            if (!product || product.stock < item.quantity) {
                return res.status(400).json({ message: `Product ${item.productId} not available or insufficient stock.` });
            }
        }

        const newOrder = new Order({
            supplierId: req.user.id,
            listItems,
            status: 'created'
        });

        const savedOrder = await newOrder.save();
        res.status(201).json(savedOrder);
    } catch (error) {
        res.status(500).json({ message: error.message });
    }
}

// update order status to "in process"
export const confirmOrder = async (req, res) => {
    const { id } = req.params;
    try {
        const order = await Order.findById(id);
        if (!order) return res.status(404).json({ message: "Order not found" });
        console.log("order", order); 
        order.status = "in process"; 
        await order.save();
        res.status(200).json(order);
    } catch (error) {
        res.status(500).json({ message: error.message });
    }
};

// update order status to "completed"
export const completeOrder = async (req, res) => {
    const { id } = req.params;
    try {
        const order = await Order.findById(id);
        if (!order) {
            return res.status(404).json({ message: "Order not found" });
        }
        order.status = "completed"; 
        await order.save();
        res.status(200).json(order);
    } catch (error) {
        res.status(500).json({ message: error.message });
    }
}

