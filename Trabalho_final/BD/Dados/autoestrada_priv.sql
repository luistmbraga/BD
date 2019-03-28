use autoestrada; 

CREATE USER 'administrador'@'localhost' IDENTIFIED BY '123456'; 
CREATE USER 'funcionario'@'localhost' IDENTIFIED BY '123456'; 
CREATE USER 'cliente'@'localhost' IDENTIFIED BY '123456'; 


GRANT ALL PRIVILEGES ON autoestrada.* TO 'administrador'@'localhost'; 

GRANT EXECUTE ON PROCEDURE insereAutoestrada TO 'administrador'@'localhost'; 
GRANT EXECUTE ON PROCEDURE insereDespesa TO 'administrador'@'localhost'; 
GRANT EXECUTE ON PROCEDURE InsereRegisto TO 'administrador'@'localhost'; 
GRANT EXECUTE ON PROCEDURE InsereTroco TO 'administrador'@'localhost'; 
GRANT EXECUTE ON PROCEDURE InsereVeiculo TO 'administrador'@'localhost'; 

GRANT EXECUTE ON PROCEDURE InsereVeiculo TO 'funcionario'@'localhost'; 
GRANT EXECUTE ON PROCEDURE InsereRegisto TO 'funcionario'@'localhost';

GRANT EXECUTE ON PROCEDURE consultaRegisto TO 'cliente'@'localhost';

