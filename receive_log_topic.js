var amqp = require('amqplib/callback_api');

var connect = null;

r = require('rethinkdb');
r.connect( {host: '10.54.217.83', db: 'csn', port: 28015}, function(err, conn) {
    if (err) throw err;
    connect = conn;
})

function ejecuta(mensaje) { 

    r.table('seiscomp')
    .filter({'event_id':mensaje['event_id']})
    .count()
    .run(connect)
    .then(function(cursor) {
        return cursor
    }).then(function(count) {
        setTimeout(function() {
            //your code to be executed after 1 second
            ingresar(mensaje,count);
          }, 1000);
        
    }).catch(function(err) {
        // process error
    })
} 

function ingresar(mensaje, count) {
    mensaje['version'] = count;
    r.table('seiscomp').insert(mensaje).run(connect)
    console.log(JSON.stringify(mensaje));
}

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
           durable: false
    
        }, function(error2, q) {
            if (error2) {
                throw error2;
            }
            channel.bindQueue(q.queue, exchange,"statistics.*");

            channel.consume(q.queue, function(msg) {
                // console.log("%s", msg.content);
                // var time = msg.content['time'];
                
                let midict = JSON.parse(msg.content.toString());
                
                console.log(`xxx : ${JSON.stringify(midict)}`);
                
                insert = {};

                let event_id = midict['event_id'];

                if (midict['from_mail']) {

                        let from_mail =  midict['from_mail'];
                        // console.log(`from_mail : ${JSON.stringify(from_mail)}`);
                        time = from_mail['time'] = ((from_mail['time']) ? new Date(from_mail['time']): null);
                        year = time.getFullYear();
                        month = time.getMonth()+1;

                        mail_time = from_mail['mail_time'] = ((from_mail['mail_time']) ? new Date(from_mail['mail_time']): null);

                        insert['email_delay'] = ((mail_time) ? Math.round((time - mail_time) / (60 * 60)) : null)
                        insert['time'] = time;
                        insert['year'] = year;
                        insert['month'] = month;
                        insert['n5']   = (((from_mail['evaluation_status'] == 'preliminary') && (from_mail['delay'] > 5)) ? true : null);
                        insert['n20']   = (((from_mail['evaluation_status'] == 'final') && (from_mail['delay'] > 20)) ? true : null);
                        insert['year'] = year;
                        insert['month'] = month;
                        insert['event_id'] = event_id;
                        insert['lat'] = from_mail['lat'];
                        insert['lon'] = from_mail['lon'];
                        insert['mag'] = from_mail['mag'];
                        insert['mag_type'] = from_mail['mag_type'];
                        insert['station_count'] = from_mail['station_count'];
                        insert['evaluation_status'] = evaluation =  from_mail['evaluation_status'];
                        insert['user'] = from_mail['user'];
                        insert['author'] = ((from_mail['author']) ? from_mail['author']: null);
                        insert['sensible'] = null;
                        insert['version'] = null;
                       
                        ejecuta(insert);                       

                }
                if (midict['from_sensitive']) {
                    let from_sensitive =  midict['from_sensitive'];
                    r.db('csn').table('seiscomp').filter({'event_id': event_id}).update({'sensible':true}).run(connect)
                    console.log(`from_sensitive yyy : ${event_id} ${JSON.stringify(from_sensitive)}`);
                }
                    
            }, {
                noAck: false
            });
        });
    });
});