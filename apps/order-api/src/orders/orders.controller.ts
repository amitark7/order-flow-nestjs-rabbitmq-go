import { Body, Controller, Post } from '@nestjs/common';
import { OrdersService } from './orders.service';
import { CreateOrderDto } from './dto/create-order.dto';

@Controller('orders')
export class OrdersController {
    constructor(private readonly orderSerice: OrdersService) {}

    @Post()
    async createOrder (@Body() orderDto : CreateOrderDto) {
        return this.orderSerice.createOrder(orderDto)
    }
}
