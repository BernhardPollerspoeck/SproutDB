using Microsoft.VisualStudio.TestPlatform.CommunicationUtilities;

namespace SproutDB.Engine.Tests;

public class Dummy { }

/*
--create database testdb
--create table users
--add column users.name string
--add column users.age number
--add column users.active boolean
--drop table oldtable 
--get users
--get users select name, age
--get users as u select u.name, u.age
--get users where age > 25
--get users where age >= 30
--get users where age < 40
--get users where age <= 35
--get users where name = 'John Doe'
--get users where name != 'Jane Smith'
--get users where age > 25 and active = true
--get users where name = 'John' or name = 'Jane'
--get users where not active = false
--get users where name in ['John', 'Jane', 'Bob']
--get users where name contains 'oh'
--count users
--count users where age > 30
--sum users.age
--sum users.age where active = true
--avg users.age
--avg users.age where name contains 'J'

get users 
  follow users.id -> orders.user_id as orders 
  where orders.total > 100 
  group by users.name 
  having count(orders) > 2 
  order by count(orders) desc 
  select users.name, count(orders) as order_count, sum(orders.total) as total_spent
  page 1 size 10

--upsert users { name: 'Alice Cooper', age: 42, active: true }
--upsert users { name: 'Bob Johnson', age: 38, active: true }
--upsert users { name: 'Alice Cooper', age: 43, active: false } on name
--delete users where name = 'Bob Johnson'

get users follow users.id -> orders.user_id as orders
get users follow users.id -> orders.user_id as orders (left)
get users follow users.id -> orders.user_id as orders (inner)
get users follow users.id -> orders.user_id as orders on orders.status = 'completed'
get users follow users.id -> orders.user_id as orders follow orders.id -> items.order_id as items

get users group by active
get users group by active select active, count() as count
get users group by active having count() > 2
get orders group by user_id select user_id, sum(total) as total_spent

--get users order by age
--get users order by age desc
--get users order by name asc, age desc

get users page 1 size 10
get users order by age desc page 2 size 5

--get orders where date last 7 days
--get orders where date this month
--get orders where date before '2024-08-01'
--get orders where date after '2024-07-15'
--get orders where date > '2024-07-01' and date < '2024-08-01'

get users 
  follow users.id -> orders.user_id as orders 
  where orders.total > 100 
  group by users.name 
  having count(orders) > 2 
  order by count(orders) desc 
  select users.name, count(orders) as order_count, sum(orders.total) as total_spent
  page 1 size 10

--upsert users [
--  { name: 'John Doe', age: 30, active: true },
--  { name: 'Jane Smith', age: 25, active: true },
--  { name: 'Bob Johnson', age: 40, active: false }
--]
--upsert users { 
--  name: 'Alice Green', 
--  age: 28, 
--  active: true, 
--  profile: { 
--    settings: { theme: 'dark', notifications: true },
--    preferences: ['email', 'sms']
--  }
--}
--get users where profile.settings.theme = 'dark'

*/