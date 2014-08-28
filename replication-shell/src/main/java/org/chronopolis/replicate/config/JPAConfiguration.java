package org.chronopolis.replicate.config;

import org.chronopolis.db.common.RestoreRepository;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import javax.sql.DataSource;
import java.sql.SQLException;

/**
 * Created by shake on 4/10/14.
 */
@Configuration
@EnableJpaRepositories(basePackages = "org.chronopolis.db",
        includeFilters = @ComponentScan.Filter(value = {RestoreRepository.class},
                                               type = FilterType.ASSIGNABLE_TYPE))
@EnableTransactionManagement
public class JPAConfiguration {

    @Bean
    public DataSource dataSource() throws SQLException {
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        // TODO: Grab from properties (environment)
        dataSource.setDriverClassName("org.h2.Driver");
        dataSource.setUrl("jdbc:h2:replication-db");
        dataSource.setUsername("h2");
        dataSource.setPassword("h2");
        return dataSource;
    }

    /*
     * These end up being autoconfigured by spring-boot
     * and it is probably easiest to leave it that way for now.
     * If we need to we can manage them ourselves later.
     */

    /*
    @Bean
    public EntityManagerFactory entityManagerFactory() throws SQLException {
        HibernateJpaVendorAdapter vendorAdapter = new HibernateJpaVendorAdapter();
        vendorAdapter.setGenerateDdl(true);
        vendorAdapter.setDatabase(Database.H2);
        vendorAdapter.setShowSql(true);

        LocalContainerEntityManagerFactoryBean factory =
                new LocalContainerEntityManagerFactoryBean();
        factory.setJpaVendorAdapter(vendorAdapter);
        factory.setPackagesToScan("org.chronopolis.db.common.model");
        factory.setDataSource(dataSource());
        factory.setJpaDialect(vendorAdapter.getJpaDialect());
        factory.setPersistenceProviderClass(HibernatePersistenceProvider.class);
        factory.afterPropertiesSet();

        return factory.getObject();
    }

    @Bean
    public EntityManager entityManager(EntityManagerFactory entityManagerFactory) {
        return entityManagerFactory.createEntityManager();
    }

    @Bean
    public PlatformTransactionManager transactionManager() throws SQLException {
        JpaTransactionManager txManager = new JpaTransactionManager();
        txManager.setEntityManagerFactory(entityManagerFactory());
        return txManager;
    }

    @Bean
    public HibernateExceptionTranslator hibernateExceptionTranslator() {
        return new HibernateExceptionTranslator();
    }
    */

}
