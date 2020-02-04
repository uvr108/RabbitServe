var amqp = require('amqplib/callback_api');

var connect = null;

r = require('rethinkdb');
r.connect( {host: '10.54.217.85', db: 'csn', port: 28015}, function(err, conn) {
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
    // console.log(JSON.stringify(mensaje));
}

amqp.connect('amqp://statistics:stdpass@thumper.lan.csn.uchile.cl:5672', function(error0, connection) {
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
            
                
                let midict = JSON.parse(msg.content.toString());
                 if (midict['data']) {
                     midict = midict['data']
                 }
                
                // console.log(`xxx : ${JSON.stringify(midict)}`);
                
                insert = {};

                let event_id = midict['event_id'];
                
                if (midict['from_mail']) {
                        
                        
                        from_mail = midict['from_mail'];

                        mail_time = from_mail['mail_time'];
                        time = from_mail['time'];

                        /*    FECHA_EMAIL    */
                        
                        let m = JSON.stringify(mail_time).substring(0,20).replace(/\"/g,'')+'+0000';
                        let n = new Date(Date.parse(m));
                        let fecha_email = n.toUTCString();
                        let epoch_email = new Date(m.substring(0,4),m.substring(5,7), m.substring(8,10), m.substring(11,13), m.substring(14,16),  m.substring(17,19)).getTime()/1000;

                        /*    FECHA_EVEN    */

                        let s = JSON.stringify(time + '+0000').replace(' ','T').replace(/\//g,'-').replace(/\"/g,'');
                        let d = new Date(Date.parse(s));
                        let fecha_even = d.toUTCString();
                        
                        let epoch_event = new Date(s.substring(0,4),s.substring(5,7), s.substring(8,10), s.substring(11,13), s.substring(14,16),  s.substring(17,19)).getTime()/1000;
      
                        let year = Number(s.substring(0,4));
                        let month = Number(s.substring(5,7));

                        let diff = Number(((epoch_email - epoch_event)/60).toFixed(2));

                        insert['fecha_even'] = d; //fecha_even;
                        insert['fecha_email'] = n; // fecha_email;
                        insert['year'] = year;
                        insert['month'] = month;
                        insert['n5']   = (((from_mail['evaluation_status'] == 'preliminary') && (from_mail['delay'] > 5)) ? true : null);
                        insert['n20']   = (((from_mail['evaluation_status'] == 'final') && (from_mail['delay'] > 20)) ? true : null);
                        insert['event_id'] = event_id;
                        insert['lat'] = from_mail['lat'];
                        insert['lon'] = from_mail['lon'];
                        insert['mag'] = from_mail['mag'];
                        insert['process_delay'] = from_mail['delay'];
                        insert['email_delay'] = diff;
                        insert['mag_type'] = from_mail['mag_type'];
                        insert['station_count'] = from_mail['station_count'];
                        insert['evaluation_status'] = evaluation =  from_mail['evaluation_status'];
                        insert['user'] = from_mail['user'];
                        insert['author'] = ((from_mail['author']) ? from_mail['author']: null);
                        insert['sensible'] = null;
                        insert['version'] = null;
                        // console.log(`TIME : ${event_id} ${from_mail['time']} ${time} : email_delat -> ${insert['email_delay']} []: ${t} - ${mail_time}`);
                        // console.log(`[event_id] ${event_id} ${s} ${d}`);
                        // console.log(`insert : ${JSON.stringify(insert)}`);     
                        ejecuta(insert);                       

                }
                if (midict['from_sensitive']) {
                    let from_sensitive =  midict['from_sensitive'];
                    r.db('csn').table('seiscomp').filter({'event_id': event_id}).update({'sensible':true}).run(connect)
                    // console.log(`from_sensitive yyy : ${event_id} ${JSON.stringify(from_sensitive)}`);
                }
            
                    
            }, {
                noAck: false
            });
        });
    });
});
