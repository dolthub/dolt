import { Entity, PrimaryKey, Property } from "@mikro-orm/core";

@Entity()
export class User {
    @PrimaryKey()
    id!: number;

    @Property()
    firstName!: string;

    @Property()
    lastName!: string;

    @Property()
    age!: number;

    constructor(firstName: string, lastName: string, age: number) {
        this.firstName = firstName;
        this.lastName = lastName;
        this.age = age;
    }
}
