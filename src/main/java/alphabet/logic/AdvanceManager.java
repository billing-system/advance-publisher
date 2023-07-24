package alphabet.logic;

import dal.TransactionRepository;
import enums.DbTransactionStatus;
import external.api.TransactionDirection;
import logger.BillingSystemLogger;
import models.db.BillingTransaction;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.LockModeType;
import java.time.Instant;
import java.util.logging.Level;

@Service
@EnableScheduling
public class AdvanceManager {

    private final TransactionRepository transactionRepository;
    private final BillingSystemLogger logger;

    @Autowired
    public AdvanceManager(TransactionRepository transactionRepository,
                          BillingSystemLogger logger) {
        this.transactionRepository = transactionRepository;
        this.logger = logger;
    }

    @Scheduled(fixedRate = 5000)
    public void manageAdvance() {
        logger.log(Level.FINE, "For our case, this schedule is dispensable; I simply desired the service " +
                "to continue running and publish advances.");
        performAdvance("tomer", 3000);
    }

    @Transactional
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    public void performAdvance(String dstBankAccount, long amount) {
        try {
            tryPerformingAdvance(dstBankAccount, amount);
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Unknown exception has occurred while performing an " +
                    "advance: " + e.getMessage());
        }
    }

    private void tryPerformingAdvance(String dstBankAccount, long amount) {
        if (isAdvanceForDstAlreadyUnderway(dstBankAccount)) {
            logger.log(Level.WARNING, "Advance is already underway for " + dstBankAccount);
        } else {
            saveAdvanceInDatabase(dstBankAccount, amount);
        }
    }

    private boolean isAdvanceForDstAlreadyUnderway(String dstBankAccount) {
        return !transactionRepository.findByBankAccountAndTransactionDirection(dstBankAccount,
                TransactionDirection.CREDIT).isEmpty();
    }

    private void saveAdvanceInDatabase(String dstBankAccount, long amount) {
        logger.log(Level.INFO, "Got new request to make advance to: " + dstBankAccount +
                ", with amount: " + amount);

        transactionRepository.save(createAdvance(dstBankAccount, amount));

        logger.log(Level.INFO, "Saved successfully in the database advance with amount "
                + amount + "to: " + dstBankAccount);
    }

    private BillingTransaction createAdvance(String dstBankAccount, long amount) {
        BillingTransaction advance = new BillingTransaction();

        advance.setTransactionTime(Instant.now());
        advance.setDstBankAccount(dstBankAccount);
        advance.setTransactionDirection(TransactionDirection.CREDIT);
        advance.setTransactionStatus(DbTransactionStatus.WAITING_TO_BE_SENT);
        advance.setAmount(amount);

        return advance;
    }
}
