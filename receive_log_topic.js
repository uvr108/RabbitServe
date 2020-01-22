var amqp = require('amqplib/callback_api');

var connect = null;

r = require('rethinkdb');
r.connect( {host: '10.54.217.83', port: 28015}, function(err, conn) {
    if (err) throw err;
    connect = conn;
})

amqp.connect('amqp://statistics:stdpass@shogouki.lan.csn.uchile.cl:5672', function(error0, connection) {
    if (error0) {
        throw error0;
    }
    connection.createChannel(function(error1, channel) {
        if (error1) {
            throw error1;
        }
        var exchange = 'STD';

        channel.assertExchange(exchange, 'topic', {
            
        });

        channel.assertQueue('std_queue', {
        
    
        }, function(error2, q) {
            if (error2) {
                throw error2;
            }
            console.log(' [*] Waiting for logs. To exit press CTRL+C');

            //args.forEach(function(key) {
            channel.bindQueue(q.queue, exchange,"statistics.*");
            //});

            channel.consume(q.queue, function(msg) {
                // console.log("%s", msg.content);
                // var time = msg.content['time'];
                let midict = JSON.parse(msg.content.toString());
                midict['time'] = ((midict['time']) ? new Date(midict['time']): null);
                midict['mail_time'] = ((midict['mail_time']) ? new Date(midict['mail_time']): null);
                r.db('csn').table('seiscomp').insert(midict).run(connect)
            }, {
                noAck: false
            });
        });
    });
});