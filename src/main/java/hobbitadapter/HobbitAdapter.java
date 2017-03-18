package hobbitadapter;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.MessageProperties;
import org.hobbit.core.Commands;
import org.hobbit.core.Constants;
import org.hobbit.core.components.AbstractCommandReceivingComponent;
import org.hobbit.core.data.RabbitQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;

/**
 * Implémentation de notre système adaptée aux benchmarks de la plate-forme Hobbit.
 * Elle reçoit les messages dans son "inputQueue" et renvoie ses sorties en "outputQueue".
 * Quand on reçoit le TERMINATION_MESSAGE, le flux d'entrée est terminé. On finit le traitement
 * des tuples reçus et à notre tour on envoie le TERMINATION_MESSAGE.
 * 
 * Ne pas oublier d'inclure les variables d'environnement (deployment/env) quand on construit une
 * image Docker à partir du Dockerfile (deployment/Dockerfile).
 * 
 * Attention aux noms pour l'inputQueue et l'outputQueue, il ne faut pas les changer.
 * 
 * Le code d'exécution de notre système doit se trouver dans execute().
 */
class HobbitAdapter extends AbstractCommandReceivingComponent {
    private static final Logger logger = LoggerFactory.getLogger(HobbitAdapter.class);
    private static final String TERMINATION_MESSAGE = "~~Termination Message~~";
    private static final Charset CHARSET = Charset.forName("UTF-8");

    private final CountDownLatch startExecutionBarrier = new CountDownLatch(1);
    private final CountDownLatch terminationMessageBarrier = new CountDownLatch(1);

    private RabbitQueue inputQueue;
    private RabbitQueue outputQueue;

    /**
     * Méthode appelée en premier lors de l'exécution du programme.
     * Elle ouvre les communications avec les différentes files RMQ.
     * Elle conduit aussi à la création du consumer pour l'input RMQ.
     */
    @Override
    public void init() throws Exception {
        logger.debug("Initializing...");
        super.init();
        String hobbitSessionId = getHobbitSessionId();
        if (hobbitSessionId.equals(Constants.HOBBIT_SESSION_ID_FOR_BROADCASTS) ||
                hobbitSessionId.equals(Constants.HOBBIT_SESSION_ID_FOR_PLATFORM_COMPONENTS)) {
            throw new IllegalStateException("Wrong hobbit session id. It must not be equal to HOBBIT_SESSION_ID_FOR_BROADCASTS or HOBBIT_SESSION_ID_FOR_PLATFORM_COMPONENTS");
        }
        initCommunications();
        logger.debug("Initialized");
    }

    /**
     * Crée les files d'entrée et sorties dans RMQ.
     * Crée le consumer pour la file d'entrée.
     * @throws Exception
     */
    private void initCommunications() throws Exception {
        outputQueue = createQueueWithName(getOutputQueueName());
        inputQueue = createQueueWithName(getInputQueueName());
        registerConsumerFor(inputQueue);
    }

    /**
     * A partir d'un nom, crée une file RMQ correspondante
     * et la connexion qui va avec.
     * @param name Nom de la file à créer.
     * @return File RMQ
     * @throws IOException
     * @throws TimeoutException
     */
    private RabbitQueue createQueueWithName(String name) throws IOException, TimeoutException {
        Channel channel = createConnection().createChannel();
        channel.basicQos(getPrefetchCount());
        channel.queueDeclare(name, false, false, true, null);
        return new RabbitQueue(channel, name);
    }

    /**
     * Crée une connexion pour RMQ en fonction d'un hôte.
     * @return Connexion à RMQ.
     * @throws IOException
     * @throws TimeoutException
     */
    private Connection createConnection() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(getHost());
        return factory.newConnection();
    }

    /**
     * Enregistre un consumer pour une file RMQ, et indique
     * ce qu'il doit faire à chaque réception de message.
     * 
     * Ici on appelle la méthode handleDelivery() sur le corps
     * du message.
     * 
     * @param queue File RMQ d'entrée.
     * @throws IOException
     */
    private void registerConsumerFor(RabbitQueue queue) throws IOException {
        Channel channel = queue.getChannel();
        channel.basicConsume(queue.getName(), false, new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag,
                                       Envelope envelope,
                                       AMQP.BasicProperties properties,
                                       byte[] body) throws IOException {
                getChannel().basicAck(envelope.getDeliveryTag(), false);
                HobbitAdapter.this.handleDelivery(body);
            }
        });
    }

    /**
     * Récupère l'hôte RMQ dans les variables d'environnement.
     * @return Nom de l'hôte ("rabbit")
     */
    private String getHost() {
        return System.getenv().get(Constants.RABBIT_MQ_HOST_NAME_KEY);
    }

    private int getPrefetchCount() {
        return 1;
    }

    /**
     * Récupère le nom de la queue d'input dans les variables d'environnement.
     * @return Nom effectif de la file d'entrée.
     */
    private String getInputQueueName() {
        return toPlatformQueueName(Constants.DATA_GEN_2_SYSTEM_QUEUE_NAME);
    }

    /**
     * Concatène un nom de file RMQ avec l'id de la session Hobbit.
     * @param queueName Nom de la file RMQ.
     * @return nomFile.idSession
     */
    private static String toPlatformQueueName(String queueName) {
        return queueName + "." + System.getenv().get(Constants.HOBBIT_SESSION_ID_KEY);
    }
    
    /**
     * Récupère le nom de la queue d'input dans les variables d'environnement.
     * @return Nom effectif de la file de sortie.
     */
    private String getOutputQueueName() {
        return toPlatformQueueName(Constants.SYSTEM_2_EVAL_STORAGE_QUEUE_NAME);
    }

    @Override
    public void run() throws Exception {
        logger.debug("Sending SYSTEM_READY_SIGNAL...");
        sendToCmdQueue(Commands.SYSTEM_READY_SIGNAL);   // Notifies PlatformController that it is ready to start
        logger.debug("Waiting for TASK_GENERATION_FINISHED...");
        startExecutionBarrier.await();
        logger.debug("Starting system execution...");
        execute();
        logger.debug("Finished");
    }

    /**
     * Pour l'instant vérifie seulement si la commande indique que le test est prêt,
     * et dans ce cas permet à notre système (dans run()) de démarrer l'exécution.
     */
    @Override
    public void receiveCommand(byte command, byte[] data) {
        if (command == Commands.TASK_GENERATION_FINISHED) {
            startExecutionBarrier.countDown();
        }
    }

    /**
     * This is where system execution starts when it receives {@code Commands.TASK_GENERATION_FINISHED}
     * from the PlatformController. Since all the processing done upon receiving a message in {@link #handleDelivery(byte[])}
     * this method is just blocked.
     */
    private void execute() throws Exception {
        try {
            logger.debug("Waiting for termination message...");
            terminationMessageBarrier.await();
            logger.debug("Sending termination message...");
            sendTerminationMessage();
        } catch (Exception e) {
            logger.error("Exception", e);
        }
        logger.debug("Execution finished.");
    }

    /**
     * Envoie le message de fin en sortie une fois que les traitements sont terminés.
     * @throws Exception
     */
    private void sendTerminationMessage() throws Exception {
        logger.debug("Sending termination message to: {} sender: {}", outputQueue.getName(), this);
        send(TERMINATION_MESSAGE);
    }

    /**
     * Envoie un message sous forme de bytes.
     * @param string Message à envoyer.
     * @throws IOException
     */
    private void send(String string) throws IOException {
        send(string.getBytes(CHARSET));
    }

    /**
     * Envoie un ensemble de bytes sur la file de sortie RMQ.
     * @param bytes Message à envoyer.
     * @throws IOException
     */
    private void send(byte[] bytes) throws IOException {
        Channel channel = outputQueue.getChannel();
        channel.basicPublish("", outputQueue.getName(), MessageProperties.PERSISTENT_BASIC, bytes);
    }

    /**
     * Indique l'action à réaliser à la réception d'un message par le consumer.
     * @param bytes Message reçu.
     */
    private void handleDelivery(byte[] bytes) {
        try {
            String message = new String(bytes, CHARSET);
            if (TERMINATION_MESSAGE.equals(message)) {
                logger.debug("Got termination message");
                terminationMessageBarrier.countDown();
            } else {
                logger.debug("Repeating message: {}", message);
                send(bytes);
            }
        } catch (Exception e) {
            logger.error("Exception", e);
        }
    }

    /**
     * Dernière fonction appelée par le programme après la fin des traitements.
     * Ferme proprement la file d'entrée RMQ puis la file de sortie RMQ.
     */
    @Override
    public void close() throws IOException {
        super.close();
        try {
            Channel channel = inputQueue.getChannel();
            Connection connection = channel.getConnection();
            channel.close();
            connection.close();
            channel = outputQueue.getChannel();
            connection = channel.getConnection();
            channel.close();
            connection.close();
        } catch (TimeoutException e) {
            logger.debug("Exception", e);
        }
    }
}