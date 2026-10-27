import { IsEmail, IsNotEmpty, IsNumber, IsPositive } from "class-validator"
import { OrderStatus } from "../order.entity"


export class CreateOrderDto {
    @IsEmail()
    @IsNotEmpty()
    customerEmail!: string

    @IsNumber()
    @IsNotEmpty()
    @IsPositive()
    amount !: number
}