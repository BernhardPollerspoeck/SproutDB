using Microsoft.VisualStudio.TestPlatform.CommunicationUtilities;

namespace SproutDB.Engine.Tests;

public class Dummy { }

/*

get users 
  follow users.id -> orders.user_id as orders 
  where orders.total > 100 
  group by users.name 
  having count(orders) > 2 
  order by count(orders) desc 
  select users.name, count(orders) as order_count, sum(orders.total) as total_spent
  page 1 size 10

get users follow users.id -> orders.user_id as orders
get users follow users.id -> orders.user_id as orders (left)
get users follow users.id -> orders.user_id as orders (inner)
get users follow users.id -> orders.user_id as orders on orders.status = 'completed'
get users follow users.id -> orders.user_id as orders follow orders.id -> items.order_id as items

get users group by active
get users group by active select active, count() as count
get users group by active having count() > 2
get orders group by user_id select user_id, sum(total) as total_spent

get users 
  follow users.id -> orders.user_id as orders 
  where orders.total > 100 
  group by users.name 
  having count(orders) > 2 
  order by count(orders) desc 
  select users.name, count(orders) as order_count, sum(orders.total) as total_spent
  page 1 size 10

*/