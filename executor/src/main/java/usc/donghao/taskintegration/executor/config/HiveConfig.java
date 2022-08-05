package usc.donghao.taskintegration.executor.config;

import org.apache.tomcat.jdbc.pool.DataSource;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.hadoop.hive.HiveClientFactory;
import org.springframework.data.hadoop.hive.HiveClientFactoryBean;
import org.springframework.data.hadoop.hive.HiveTemplate;
import org.springframework.jdbc.core.JdbcTemplate;

@Configuration
public class HiveConfig {
    @Value("${hiveConnectionURL}")
    private String hiveConnectionURL;
    @Value("${username}")
    private String username;
    @Value("${password}")
    private String password;

    @Bean(name="hiveDataSource")
    @Qualifier("hiveJdbcDataSource")
    public DataSource dataSource(){
        DataSource dataSource=new DataSource();
        dataSource.setUrl(this.hiveConnectionURL);
        dataSource.setDriverClassName("org.apache.hive.jdbc.HiveDriver");
        dataSource.setUsername(this.username);
        dataSource.setPassword(this.password);
        dataSource.setTestOnBorrow(true);
        dataSource.setValidationQuery("select 1");
        return dataSource;
    }

    @Bean(name="hiveJdbcTemplate")
    public JdbcTemplate hiveJdbcTemplate(@Qualifier("hiveJdbcDataSource")DataSource dataSource){
        return new JdbcTemplate(dataSource());
    }
    @Bean(name="hiveClientFactory")
    public HiveClientFactory hiveClientFactory(@Qualifier("hiveJdbcDataSource") DataSource dataSource) throws Exception{
        HiveClientFactoryBean bean=new HiveClientFactoryBean();
        bean.setHiveDataSource(dataSource);
        bean.afterPropertiesSet();
        return bean.getObject();
    }

    @Bean
    public HiveTemplate hiveTemplate(@Qualifier("hiveClientFactory")HiveClientFactory hiveClientFactory){
        return new HiveTemplate(hiveClientFactory);
    }
}

